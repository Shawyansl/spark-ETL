import logging

from pyspark.sql import DataFrame, SparkSession

from src.write_outputs import OUTPUT_DIR
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

def validate_revenue_reconciliation(
    completed_sales: DataFrame,
    total_revenue: DataFrame,
) -> None:
    detail_total = (
        completed_sales
        .agg(F.sum("net_amount").alias("total"))
        .first()["total"]
    )

    report_total = total_revenue.first()["total_net_revenue"]

    if detail_total is None:
        detail_total = 0

    if report_total is None:
        report_total = 0

    if detail_total != report_total:
        raise ValueError(
            f"Revenue mismatch: completed sales total={detail_total}, "
            f"reported total={report_total}"
        )

    logger.info("Passed revenue reconciliation: %s", detail_total)

def validate_sales_references(
    fact_sales: DataFrame,
    cleaned_customers: DataFrame,
    cleaned_products: DataFrame,
) -> None:
    customer_keys = cleaned_customers.select("customer_id").distinct()
    invalid_customers = (
        fact_sales.select("customer_id")
        .distinct()
        .join(customer_keys, on="customer_id", how="left_anti")
        .limit(5)
        .collect()
    )

    if invalid_customers:
        raise ValueError(
            f"Unknown customer_id values in fact_sales: {invalid_customers}"
        )

    product_keys = cleaned_products.select("product_id").distinct()
    invalid_products = (
        fact_sales.select("product_id")
        .distinct()
        .join(product_keys, on="product_id", how="left_anti")
        .limit(5)
        .collect()
    )

    if invalid_products:
        raise ValueError(
            f"Unknown product_id values in fact_sales: {invalid_products}"
        )

    logger.info("Passed sales customer/product reference validation")

def validate_completed_sales_only(completed_sales: DataFrame) -> None:
    invalid_rows = completed_sales.filter(
        F.col("status").isNull()
        | (F.col("status") != "completed")
    )

    examples = invalid_rows.select("order_id", "status").limit(5).collect()

    if examples:
        raise ValueError(
            f"Non-completed orders found in revenue input: {examples}"
        )

    logger.info("Passed completed-sales status validation")

def validate_discount_range(fact_sales: DataFrame) -> None:
    invalid_rows = fact_sales.filter(
        F.col("discount_pct").isNull()
        | (F.col("discount_pct") < 0)
        | (F.col("discount_pct") > 1)
    )

    examples = invalid_rows.select(
        "order_id",
        "product_id",
        "discount_pct",
    ).limit(5).collect()

    if examples:
        raise ValueError(
            f"discount_pct outside [0, 1] in fact_sales: {examples}"
        )

    logger.info("Passed discount_pct range validation")

def validate_sales_values(fact_sales: DataFrame) -> None:
    invalid_rows = fact_sales.filter(
        (F.col("quantity") <= 0) | (F.col("unit_price") < 0)
    )

    examples = invalid_rows.select(
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
    ).limit(5).collect()

    if examples:
        raise ValueError(
            f"Invalid quantity or unit_price found in fact_sales: {examples}"
        )

    logger.info("Passed quantity and unit_price validation")


def validate_unique_customer_ids(customers: DataFrame) -> None:
    duplicate_keys = (
        customers
        .groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
    )

    duplicates = duplicate_keys.limit(5).collect()

    if duplicates:
        duplicate_ids = [row["customer_id"] for row in duplicates]
        raise ValueError(
            f"Duplicate customer_id values in cleaned_customers: {duplicate_ids}"
        )

    logger.info("Passed customer_id uniqueness check")


def validate_outputs(spark: SparkSession, expected_outputs: dict[str, DataFrame]) -> None:
    for name, expected_df in expected_outputs.items():
        path = OUTPUT_DIR / name

        if not (path / "_SUCCESS").exists():
            raise RuntimeError(f"Output is missing or incomplete: {path}")

        actual_df = spark.read.parquet(str(path))

        expected_count = expected_df.count()
        actual_count = actual_df.count()

        if actual_count != expected_count:
            raise ValueError(
                f"Row count mismatch for {name}: "
                f"expected {expected_count}, found {actual_count}"
            )

        logger.info(
            "Validated output %s: %d rows",
            name,
            actual_count,
        )