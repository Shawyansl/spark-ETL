from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def clean_customers(customers: DataFrame) -> DataFrame:
    cleaned = (
        customers
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )

    normalized_city = F.trim(F.col("city"))
    cleaned = cleaned.withColumn(
        "city",
        F.when(
            normalized_city.isNull() | (normalized_city == ""),
            F.lit("Unknown"),
        ).otherwise(normalized_city),
    )
    return cleaned.withColumn(
        "signup_date",
        F.to_date(F.try_to_timestamp(F.col("signup_date"), F.lit("yyyy-MM-dd"))),
    )


def clean_products(products: DataFrame) -> DataFrame:
    return (
        products
        .filter(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
    )


def clean_orders(
    orders: DataFrame,
    cleaned_customers: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    source_columns = orders.columns
    trimmed_status = F.trim(F.col("status"))
    normalized = orders.withColumn(
        "status",
        F.when(
            trimmed_status.isNull() | (trimmed_status == ""),
            F.lit(None).cast("string"),
        ).otherwise(F.lower(trimmed_status)),
    )

    # Exact duplicate source rows are quarantined after status normalization.
    exact_duplicate_window = (
        Window
        .partitionBy(*normalized.columns)
        .orderBy(F.lit(1))
    )
    ranked_source = normalized.withColumn(
        "_source_rank",
        F.row_number().over(exact_duplicate_window),
    )
    duplicate_source_rows = (
        ranked_source
        .filter(F.col("_source_rank") > 1)
        .withColumn("reject_reason", F.lit("duplicate_order"))
        .select(*source_columns, "reject_reason")
    )
    unique_source_rows = ranked_source.filter(F.col("_source_rank") == 1).drop(
        "_source_rank"
    )

    parsed = unique_source_rows.withColumn(
        "_parsed_order_date",
        F.to_date(
            F.try_to_timestamp(F.col("order_date"), F.lit("yyyy-MM-dd"))
        ),
    )
    customer_keys = (
        cleaned_customers
        .filter(F.col("customer_id").isNotNull())
        .select(F.col("customer_id").alias("_valid_customer_id"))
        .distinct()
    )
    checked = parsed.join(
        customer_keys,
        parsed["customer_id"] == customer_keys["_valid_customer_id"],
        "left",
    )

    reason = (
        F.when(F.col("order_id").isNull(), F.lit("missing_order_id"))
        .when(F.col("customer_id").isNull(), F.lit("missing_customer_id"))
        .when(F.col("order_date").isNull(), F.lit("missing_order_date"))
        .when(F.col("_parsed_order_date").isNull(), F.lit("invalid_order_date"))
        .when(F.col("status").isNull(), F.lit("missing_status"))
        .when(
            ~F.col("status").isin("completed", "cancelled", "returned"),
            F.lit("invalid_status"),
        )
        .when(F.col("_valid_customer_id").isNull(), F.lit("orphan_customer"))
    )
    checked = checked.withColumn("reject_reason", reason)

    invalid_orders = (
        checked
        .filter(F.col("reject_reason").isNotNull())
        .select(*source_columns, "reject_reason")
    )
    valid_orders = checked.filter(F.col("reject_reason").isNull())

    order_versions = (
        valid_orders
        .groupBy("order_id")
        .agg(
            F.countDistinct(
                F.struct(
                    "customer_id",
                    "_parsed_order_date",
                    "status",
                    "payment_method",
                )
            ).alias("_payload_versions")
        )
    )
    orders_with_versions = valid_orders.join(order_versions, on="order_id")
    conflicting_orders = (
        orders_with_versions
        .filter(F.col("_payload_versions") > 1)
        .withColumn("reject_reason", F.lit("conflicting_duplicate_order"))
        .select(*source_columns, "reject_reason")
    )

    same_order_window = (
        Window
        .partitionBy("order_id")
        .orderBy("customer_id", "_parsed_order_date", "status", "payment_method")
    )
    ranked_valid_orders = (
        orders_with_versions
        .filter(F.col("_payload_versions") == 1)
        .withColumn("_duplicate_rank", F.row_number().over(same_order_window))
    )
    repeated_valid_orders = (
        ranked_valid_orders
        .filter(F.col("_duplicate_rank") > 1)
        .withColumn("reject_reason", F.lit("duplicate_order"))
        .select(*source_columns, "reject_reason")
    )
    cleaned_orders = (
        ranked_valid_orders
        .filter(F.col("_duplicate_rank") == 1)
        .drop(
            "order_date",
            "_valid_customer_id",
            "reject_reason",
            "_payload_versions",
            "_duplicate_rank",
        )
        .withColumnRenamed("_parsed_order_date", "order_date")
    )

    quarantine_orders = (
        duplicate_source_rows
        .unionByName(invalid_orders)
        .unionByName(conflicting_orders)
        .unionByName(repeated_valid_orders)
    )
    return cleaned_orders, quarantine_orders


def clean_order_items(
    order_items: DataFrame,
    cleaned_orders: DataFrame,
    cleaned_products: DataFrame,
    quarantine_orders: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    source_columns = order_items.columns
    wide_decimal = DecimalType(20, 10)

    parsed = (
        order_items
        .withColumn("_raw_quantity", F.col("quantity"))
        .withColumn("_raw_unit_price", F.col("unit_price"))
        .withColumn("_raw_discount_pct", F.col("discount_pct"))
        .withColumn("_quantity_wide", F.expr("try_cast(quantity AS DECIMAL(20,10))"))
        .withColumn("_parsed_quantity", F.expr("try_cast(quantity AS INT)"))
        .withColumn("_unit_price_wide", F.expr("try_cast(unit_price AS DECIMAL(20,10))"))
        .withColumn("_parsed_unit_price", F.expr("try_cast(_unit_price_wide AS DECIMAL(10,2))"))
        .withColumn(
            "_discount_pct_wide",
            F.expr("try_cast(discount_pct AS DECIMAL(20,10))"),
        )
        .withColumn(
            "_parsed_discount_pct",
            F.expr("try_cast(_discount_pct_wide AS DECIMAL(10,4))"),
        )
        .withColumn("quantity", F.col("_parsed_quantity"))
        .withColumn("unit_price", F.col("_parsed_unit_price"))
        .withColumn("discount_pct", F.col("_parsed_discount_pct"))
    )

    valid_order_ids = (
        cleaned_orders
        .filter(F.col("order_id").isNotNull())
        .select(F.col("order_id").alias("_valid_order_id"))
        .distinct()
    )
    rejected_order_ids = (
        quarantine_orders
        .filter(F.col("order_id").isNotNull())
        .select(F.col("order_id").alias("_rejected_order_id"))
        .distinct()
    )
    valid_product_ids = (
        cleaned_products
        .filter(F.col("product_id").isNotNull())
        .select(F.col("product_id").alias("_valid_product_id"))
        .distinct()
    )

    checked = parsed.join(
        valid_order_ids,
        parsed["order_id"] == valid_order_ids["_valid_order_id"],
        "left",
    )
    checked = checked.join(
        rejected_order_ids,
        checked["order_id"] == rejected_order_ids["_rejected_order_id"],
        "left",
    )
    checked = checked.join(
        valid_product_ids,
        checked["product_id"] == valid_product_ids["_valid_product_id"],
        "left",
    )

    quantity_is_integral = (
        F.col("_quantity_wide")
        == F.col("_parsed_quantity").cast(wide_decimal)
    )
    price_is_exact_scale = (
        F.col("_parsed_unit_price").cast(wide_decimal)
        == F.col("_unit_price_wide")
    )
    discount_is_exact_scale = (
        F.col("_parsed_discount_pct").cast(wide_decimal)
        == F.col("_discount_pct_wide")
    )

    reason = (
        F.when(F.col("order_id").isNull(), F.lit("missing_order_id"))
        .when(F.col("product_id").isNull(), F.lit("missing_product_id"))
        .when(F.col("_raw_quantity").isNull(), F.lit("missing_quantity"))
        .when(
            F.col("_quantity_wide").isNull()
            | F.col("_parsed_quantity").isNull()
            | ~quantity_is_integral
            | (F.col("_parsed_quantity") <= 0),
            F.lit("invalid_quantity"),
        )
        .when(F.col("_raw_unit_price").isNull(), F.lit("missing_unit_price"))
        .when(
            F.col("_unit_price_wide").isNull()
            | F.col("_parsed_unit_price").isNull(),
            F.lit("invalid_unit_price"),
        )
        .when(~price_is_exact_scale, F.lit("invalid_unit_price_precision"))
        .when(F.col("_parsed_unit_price") < 0, F.lit("invalid_unit_price"))
        .when(F.col("_raw_discount_pct").isNull(), F.lit("missing_discount_pct"))
        .when(
            F.col("_discount_pct_wide").isNull()
            | F.col("_parsed_discount_pct").isNull(),
            F.lit("invalid_discount_pct"),
        )
        .when(~discount_is_exact_scale, F.lit("invalid_discount_pct_precision"))
        .when(
            (F.col("_parsed_discount_pct") < 0)
            | (F.col("_parsed_discount_pct") > 1),
            F.lit("invalid_discount_pct"),
        )
        .when(
            (F.col("_valid_order_id").isNull())
            & F.col("_rejected_order_id").isNotNull(),
            F.lit("order_quarantined"),
        )
        .when(F.col("_valid_order_id").isNull(), F.lit("orphan_order"))
        .when(F.col("_valid_product_id").isNull(), F.lit("orphan_product"))
    )
    checked = checked.withColumn("reject_reason", reason)

    def as_raw_quarantine(df: DataFrame) -> DataFrame:
        return (
            df.withColumn("quantity", F.col("_raw_quantity"))
            .withColumn("unit_price", F.col("_raw_unit_price"))
            .withColumn("discount_pct", F.col("_raw_discount_pct"))
            .select(*source_columns, "reject_reason")
        )

    other_quarantine = as_raw_quarantine(
        checked.filter(F.col("reject_reason").isNotNull())
    )
    valid_rows = checked.filter(F.col("reject_reason").isNull())

    key_stats = (
        valid_rows
        .groupBy("order_id", "product_id")
        .agg(
            F.count("*").alias("_key_row_count"),
            F.countDistinct(
                F.struct("quantity", "unit_price", "discount_pct")
            ).alias("_payload_versions"),
        )
    )
    rows_with_stats = valid_rows.join(
        key_stats,
        on=["order_id", "product_id"],
        how="inner",
    )
    conflicting_rows = (
        rows_with_stats
        .filter(F.col("_payload_versions") > 1)
        .withColumn(
            "reject_reason",
            F.lit("conflicting_duplicate_order_item"),
        )
    )

    duplicate_window = (
        Window
        .partitionBy("order_id", "product_id")
        .orderBy("quantity", "unit_price", "discount_pct")
    )
    ranked_rows = (
        rows_with_stats
        .filter(F.col("_payload_versions") == 1)
        .withColumn("_duplicate_rank", F.row_number().over(duplicate_window))
    )
    duplicate_rows = (
        ranked_rows
        .filter(F.col("_duplicate_rank") > 1)
        .withColumn("reject_reason", F.lit("duplicate_order_item"))
    )
    cleaned_order_items = (
        ranked_rows
        .filter(F.col("_duplicate_rank") == 1)
        .drop(
            "reject_reason",
            "_valid_order_id",
            "_rejected_order_id",
            "_valid_product_id",
            "_raw_quantity",
            "_raw_unit_price",
            "_raw_discount_pct",
            "_quantity_wide",
            "_parsed_quantity",
            "_unit_price_wide",
            "_parsed_unit_price",
            "_discount_pct_wide",
            "_parsed_discount_pct",
            "_key_row_count",
            "_payload_versions",
            "_duplicate_rank",
        )
    )
    duplicate_quarantine = as_raw_quarantine(duplicate_rows)
    conflict_quarantine = as_raw_quarantine(conflicting_rows)
    quarantine_order_items = (
        other_quarantine
        .unionByName(conflict_quarantine)
        .unionByName(duplicate_quarantine)
    )
    return cleaned_order_items, quarantine_order_items
