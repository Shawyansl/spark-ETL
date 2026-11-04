import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"
logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {
    "cleaned_customers": {"customer_id", "signup_date"},
    "cleaned_products": {"product_id"},
    "cleaned_orders": {"order_id", "customer_id", "order_date", "status"},
    "cleaned_order_items": {"order_id", "product_id", "quantity", "unit_price", "discount_pct"},
    "quarantine_customers": {"customer_id", "reject_reason"},
    "quarantine_products": {"product_id", "reject_reason"},
    "quarantine_orders": {"order_id", "reject_reason"},
    "quarantine_order_items": {"order_id", "product_id", "reject_reason"},
    "fact_sales": {"order_id", "order_month", "net_amount", "status"},
    "total_revenue": {"total_net_revenue"},
    "daily_revenue": {"order_date", "net_revenue"},
    "category_revenue": {"category", "net_revenue"},
    "city_revenue": {"city", "net_revenue"},
    "completed_orders_per_day": {"order_date", "completed_orders"},
    "top_customers": {"customer_id", "net_revenue"},
}


def _examples(df: DataFrame, columns: list[str]) -> list:
    return df.select(*columns).limit(5).collect()


def validate_unique_key(df: DataFrame, key: str, label: str) -> None:
    nulls = df.filter(F.col(key).isNull()).limit(1).count()
    duplicates = df.groupBy(key).count().filter(F.col("count") > 1).limit(5).collect()
    if nulls or duplicates:
        raise ValueError(f"{label}.{key} must be non-null and unique; duplicates={duplicates}, nulls={nulls}")
    logger.info("Passed %s.%s uniqueness/non-null check", label, key)


def validate_sales_values(fact_sales: DataFrame) -> None:
    invalid = fact_sales.filter(
        F.col("quantity").isNull() | (F.col("quantity") <= 0)
        | F.col("unit_price").isNull() | (F.col("unit_price") < 0)
    )
    rows = _examples(invalid, ["order_id", "product_id", "quantity", "unit_price"])
    if rows:
        raise ValueError(f"Invalid quantity/unit_price in fact_sales: {rows}")
    logger.info("Passed sales quantity and price validation")


def validate_discount_range(fact_sales: DataFrame) -> None:
    invalid = fact_sales.filter(
        F.col("discount_pct").isNull()
        | (F.col("discount_pct") < 0)
        | (F.col("discount_pct") > 1)
    )
    rows = _examples(invalid, ["order_id", "product_id", "discount_pct"])
    if rows:
        raise ValueError(f"discount_pct outside [0, 1] in fact_sales: {rows}")
    logger.info("Passed discount_pct range validation")


def validate_clean_references(
    cleaned_orders: DataFrame,
    cleaned_customers: DataFrame,
    cleaned_order_items: DataFrame,
    cleaned_products: DataFrame,
) -> None:
    relationships = [
        (cleaned_orders, "customer_id", cleaned_customers, "customer_id", "orders.customer_id"),
        (cleaned_order_items, "order_id", cleaned_orders, "order_id", "order_items.order_id"),
        (cleaned_order_items, "product_id", cleaned_products, "product_id", "order_items.product_id"),
    ]
    for child, fk, parent, pk, label in relationships:
        parent_keys = parent.select(F.col(pk).alias("_parent_key")).distinct()
        invalid = child.filter(F.col(fk).isNull()).limit(5).collect()
        if not invalid:
            invalid = child.join(
                parent_keys, child[fk] == parent_keys["_parent_key"], "left_anti"
            ).limit(5).collect()
        if invalid:
            raise ValueError(f"Invalid reference {label}; examples: {invalid}")
    logger.info("Passed cleaned customer/order/product reference validation")



def validate_fact_grain(
    fact_sales: DataFrame,
    cleaned_order_items: DataFrame,
    cleaned_orders: DataFrame,
    cleaned_customers: DataFrame,
    cleaned_products: DataFrame,
) -> None:
    expected_lines = cleaned_order_items.count()
    actual_rows = fact_sales.count()
    duplicate_lines = (
        fact_sales.groupBy("order_id", "product_id")
        .count().filter(F.col("count") != 1).limit(5).collect()
    )
    if expected_lines != actual_rows or duplicate_lines:
        raise ValueError(
            f"Fact grain mismatch: {expected_lines} cleaned lines, "
            f"{actual_rows} fact rows; duplicate keys={duplicate_lines}"
        )

    relationships = [
        (fact_sales, "order_id", cleaned_orders, "order_id"),
        (fact_sales, "customer_id", cleaned_customers, "customer_id"),
        (fact_sales, "product_id", cleaned_products, "product_id"),
    ]
    for child, fk, parent, pk in relationships:
        parent_keys = parent.select(F.col(pk).alias("_parent_key")).distinct()
        invalid = child.filter(F.col(fk).isNull()).limit(5).collect()
        if not invalid:
            invalid = child.join(
                parent_keys, child[fk] == parent_keys["_parent_key"], "left_anti"
            ).limit(5).collect()
        if invalid:
            raise ValueError(
                f"Persisted fact has invalid {fk} reference; examples: {invalid}"
            )
    logger.info("Passed persisted fact grain and reference validation")


def validate_completed_reports(
    fact_sales: DataFrame,
    reports: dict[str, DataFrame],
) -> None:
    completed = fact_sales.filter(F.col("status") == "completed")
    expected_total = completed.agg(
        F.coalesce(F.sum("net_amount"), F.lit(0)).alias("total")
    ).first()["total"]
    report_total = reports["total_revenue"].first()["total_net_revenue"]
    if expected_total != report_total:
        raise ValueError(
            f"Completed revenue mismatch: fact={expected_total}, report={report_total}"
        )
    logger.info("Passed completed-only revenue reconciliation: %s", expected_total)


def validate_revenue_reconciliation(reports: dict[str, DataFrame]) -> None:
    total = reports["total_revenue"].first()["total_net_revenue"]
    for name in ("daily_revenue", "category_revenue", "city_revenue"):
        subtotal = reports[name].agg(F.coalesce(F.sum("net_revenue"), F.lit(0))).first()[0]
        if subtotal != total:
            raise ValueError(f"{name} does not reconcile to total_revenue: {subtotal} != {total}")
    logger.info("Passed daily/category/city revenue reconciliation")


def validate_outputs(spark: SparkSession, output_names: list[str]) -> dict[str, DataFrame]:
    """Read persisted outputs and validate completion markers and required columns."""
    persisted = {}
    for name in output_names:
        path = OUTPUT_DIR / name
        if not (path / "_SUCCESS").is_file():
            raise RuntimeError(f"Output is missing or incomplete: {path}")
        actual = spark.read.parquet(str(path))
        required = REQUIRED_COLUMNS.get(name, set())
        missing = required - set(actual.columns)
        if missing:
            raise ValueError(f"Output {name} is missing columns: {sorted(missing)}")
        row_count = actual.count()
        persisted[name] = actual
        logger.info("Validated persisted output %s: %d rows", name, row_count)
    return persisted
