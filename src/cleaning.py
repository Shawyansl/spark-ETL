from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_customers(customers: DataFrame) -> DataFrame:
    cleaned = customers.dropDuplicates(["customer_id"])

    cleaned = cleaned.withColumn(
        "city",
        F.when(F.col("city").isNull(), F.lit("Unknown"))
        .otherwise(F.col("city")),
    )

    cleaned = cleaned.withColumn(
        "signup_date",
        F.to_date(
            F.try_to_timestamp(
                F.col("signup_date"),
                F.lit("yyyy-MM-dd"),
            )
        ),
    )

    return cleaned

def clean_products(products: DataFrame) -> DataFrame:
    return products.dropDuplicates(["product_id"])

def clean_orders(orders: DataFrame, cleaned_customers: DataFrame):

    """parse order_date to a proper date format, keeping invalid dates as NULL."""
    parsed = orders.withColumn(
        "_parsed_order_date",
        F.to_date(
            F.try_to_timestamp(
                F.col("order_date"),
                F.lit("yyyy-MM-dd"),
            )
        ),
    )

    """only keep orders that have a valid customer_id in the cleaned customers table."""
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

    """add a reject_reason column to indicate why an order is invalid, if applicable."""
    reason = (
        F.when(F.col("order_id").isNull(), F.lit("missing_order_id"))
        .when(F.col("customer_id").isNull(), F.lit("missing_customer_id"))
        .when(F.col("order_date").isNull(), F.lit("missing_order_date"))
        .when(
            F.col("_parsed_order_date").isNull(),
            F.lit("invalid_order_date"),
        )
        .when(F.col("_valid_customer_id").isNull(), F.lit("orphan_customer"))
    )
    checked = checked.withColumn("reject_reason", reason)

    """create a quarantine_orders DataFrame containing only the rejected orders, dropping the temporary columns."""
    quarantine_orders = (
        checked
        .filter(F.col("reject_reason").isNotNull())
        .drop("_parsed_order_date", "_valid_customer_id")
    )
    valid_orders = checked.filter(F.col("reject_reason").isNull())
    valid_count = valid_orders.count()

    """drop duplicate order_id rows from the valid orders, keeping only the first occurrence."""
    deduplicated = valid_orders.dropDuplicates(["order_id"])
    duplicates_removed = valid_count - deduplicated.count()

    """cleaned_orders is the final DataFrame containing only valid,
             deduplicated orders,
             with the parsed order_date and 
             without the temporary columns."""
    cleaned_orders = (
        deduplicated
        .drop("order_date", "_valid_customer_id", "reject_reason")
        .withColumnRenamed("_parsed_order_date", "order_date")
    )

    return cleaned_orders, quarantine_orders, duplicates_removed

def clean_order_items(order_items: DataFrame, cleaned_orders: DataFrame, cleaned_products: DataFrame):
    """for each order_items row, check if the order_id and product_id exist in the cleaned orders and products tables, respectively."""
    valid_order_ids = (
        cleaned_orders
        .filter(F.col("order_id").isNotNull())
        .select(F.col("order_id").alias("_valid_order_id"))
        .distinct()
    )

    valid_product_ids = (
        cleaned_products
        .filter(F.col("product_id").isNotNull())
        .select(F.col("product_id").alias("_valid_product_id"))
        .distinct()
    )

    checked = order_items.join(
        valid_order_ids,
        order_items["order_id"] == valid_order_ids["_valid_order_id"],
        "left",
    )

    checked = checked.join(
        valid_product_ids,
        checked["product_id"] == valid_product_ids["_valid_product_id"],
        "left",
    )

    reason = (
        F.when(F.col("order_id").isNull(), F.lit("missing_order_id"))
        .when(F.col("product_id").isNull(), F.lit("missing_product_id"))
        .when(F.col("quantity").isNull(), F.lit("missing_quantity"))
        .when(F.col("unit_price").isNull(), F.lit("missing_unit_price"))
        .when(F.col("discount_pct").isNull(), F.lit("missing_discount_pct"))
        .when(F.col("quantity") <= 0, F.lit("invalid_quantity"))
        .when(F.col("unit_price") < 0, F.lit("invalid_unit_price"))
        .when(
            (F.col("discount_pct") < 0) | (F.col("discount_pct") > 1),
            F.lit("invalid_discount_pct"),
        )
        .when(F.col("_valid_order_id").isNull(), F.lit("orphan_order"))
        .when(F.col("_valid_product_id").isNull(), F.lit("orphan_product"))
    )

    checked = checked.withColumn("reject_reason", reason)

    quarantine_order_items = (
        checked
        .filter(F.col("reject_reason").isNotNull())
        .drop("_valid_order_id", "_valid_product_id")
    )

    cleaned_order_items = (
        checked
        .filter(F.col("reject_reason").isNull())
        .drop("reject_reason", "_valid_order_id", "_valid_product_id")
    )

    return cleaned_order_items, quarantine_order_items

if __name__ == "__main__":
    import logging
    from pyspark.sql import SparkSession
    from src.read_data import load_data

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    spark = (
        SparkSession.builder
        .appName("OnlineStoreETL")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Starting OnlineStoreETL data cleaning")

        tables = load_data(spark)
        """for customers, clean the data by dropping duplicates, replacing NULL cities with 'Unknown', and parsing signup dates."""
        cleaned_customers = clean_customers(tables["customers"])
        logger.info("Cleaned customers schema: %s", cleaned_customers.schema.simpleString())
        cleaned_customers.show(truncate=False)

        """for products, clean the data by dropping duplicates."""
        cleaned_products = clean_products(tables["products"])
        logger.info("Cleaned products schema: %s", cleaned_products.schema.simpleString())
        cleaned_products.show(truncate=False)

        """for orders, clean the data by parsing order dates, checking for valid customer_ids, and removing duplicates."""
        cleaned_orders, quarantine_orders, duplicates_removed = clean_orders(tables["orders"], cleaned_customers)
        logger.info("Cleaned orders schema: %s", cleaned_orders.schema.simpleString())
        cleaned_orders.show(truncate=False)
        logger.info("Quarantine orders schema: %s", quarantine_orders.schema.simpleString())
        quarantine_orders.show(truncate=False)
        logger.info("Removed %d duplicate orders", duplicates_removed)

        """for order items, clean the data by checking for valid order_ids and product_ids."""
        cleaned_items, quarantine_items = clean_order_items(
            tables["order_items"],
            cleaned_orders,
            cleaned_products,
        )
        
        logger.info("Cleaned order items: %d", cleaned_items.count())
        logger.info("Quarantined order items: %d", quarantine_items.count())
        
        quarantine_items.groupBy("reject_reason").count().show(truncate=False)   

        """"""     

    except Exception:
        logger.exception("OnlineStoreETL data cleaning failed")
        raise
    finally:
        spark.stop()
        logger.info("Stopped SparkSession")