import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.cleaning import (
    clean_customers,
    clean_order_items,
    clean_orders,
    clean_products,
)
from src.data_quality import (
    analyze_data_quality,
    summarize_quarantines,
    write_quality_report,
)
from src.joins import build_fact_sales
from src.read_data import load_data
from src.sales_report import add_amounts, build_reports
from src.write_outputs import write_outputs
from src.validate_outputs import (
    validate_clean_references,
    validate_completed_reports,
    validate_discount_range,
    validate_fact_grain,
    validate_outputs,
    validate_revenue_reconciliation,
    validate_sales_values,
    validate_unique_key,
)

logger = logging.getLogger(__name__)


def _missing_key_quarantine(df, key: str, reason: str):
    return (
        df.filter(F.col(key).isNull())
        .withColumn("reject_reason", F.lit(reason))
    )


def run_pipeline() -> None:
    """Run the local batch ETL from source CSVs through validated Parquet."""
    spark = (
        SparkSession.builder
        .appName("OnlineStoreETL")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    cached = []

    def cache(df):
        cached_df = df.cache()
        cached.append(cached_df)
        return cached_df

    try:
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Starting OnlineStoreETL pipeline")
        tables = load_data(spark)
        for name, df in tables.items():
            logger.info("Raw schema — %s", name)
            df.printSchema()
        quality_metrics = analyze_data_quality(tables)

        # Quarantine missing dimension keys explicitly before cleaning.
        quarantine_customers = _missing_key_quarantine(
            tables["customers"], "customer_id", "missing_customer_id"
        )
        quarantine_products = _missing_key_quarantine(
            tables["products"], "product_id", "missing_product_id"
        )
        cleaned_customers = cache(clean_customers(tables["customers"]))
        cleaned_products = cache(clean_products(tables["products"]))
        validate_unique_key(cleaned_customers, "customer_id", "cleaned_customers")
        validate_unique_key(cleaned_products, "product_id", "cleaned_products")

        cleaned_orders, quarantine_orders = clean_orders(
            tables["orders"], cleaned_customers
        )
        cleaned_orders = cache(cleaned_orders)
        quarantine_orders = cache(quarantine_orders)
        cleaned_order_items, quarantine_order_items = clean_order_items(
            tables["order_items"],
            cleaned_orders,
            cleaned_products,
            quarantine_orders,
        )
        cleaned_order_items = cache(cleaned_order_items)
        quarantine_order_items = cache(quarantine_order_items)
        logger.info("Cleaned customers: %d", cleaned_customers.count())
        logger.info("Quarantined customers: %d", quarantine_customers.count())
        logger.info("Cleaned products: %d", cleaned_products.count())
        logger.info("Quarantined products: %d", quarantine_products.count())
        logger.info("Cleaned orders: %d", cleaned_orders.count())
        logger.info("Quarantined orders: %d", quarantine_orders.count())
        logger.info("Cleaned order-item rows: %d", cleaned_order_items.count())
        logger.info("Quarantined order-item rows: %d", quarantine_order_items.count())

        validate_clean_references(
            cleaned_orders,
            cleaned_customers,
            cleaned_order_items,
            cleaned_products,
        )

        fact_sales = cache(build_fact_sales(
            cleaned_orders,
            cleaned_customers,
            cleaned_order_items,
            cleaned_products,
        ))
        validate_sales_values(fact_sales)
        validate_discount_range(fact_sales)
        sales_with_amounts = cache(add_amounts(fact_sales))
        reports = build_reports(sales_with_amounts)

        outputs = {
            "cleaned_customers": cleaned_customers,
            "cleaned_products": cleaned_products,
            "cleaned_orders": cleaned_orders,
            "cleaned_order_items": cleaned_order_items,
            "quarantine_customers": quarantine_customers,
            "quarantine_products": quarantine_products,
            "quarantine_orders": quarantine_orders,
            "quarantine_order_items": quarantine_order_items,
            "fact_sales": sales_with_amounts,
            **reports,
        }
        write_outputs(outputs)

        # Validate the Parquet data actually written, not its lazy source plans.
        persisted = validate_outputs(spark, list(outputs))
        persisted_reports = {
            name: persisted[name]
            for name in reports
        }
        validate_unique_key(
            persisted["cleaned_customers"], "customer_id", "persisted cleaned_customers"
        )
        validate_unique_key(
            persisted["cleaned_products"], "product_id", "persisted cleaned_products"
        )
        validate_clean_references(
            persisted["cleaned_orders"],
            persisted["cleaned_customers"],
            persisted["cleaned_order_items"],
            persisted["cleaned_products"],
        )
        validate_sales_values(persisted["fact_sales"])
        validate_discount_range(persisted["fact_sales"])
        validate_fact_grain(
            persisted["fact_sales"],
            persisted["cleaned_order_items"],
            persisted["cleaned_orders"],
            persisted["cleaned_customers"],
            persisted["cleaned_products"],
        )
        validate_completed_reports(persisted["fact_sales"], persisted_reports)
        validate_revenue_reconciliation(persisted_reports)

        quarantine_metrics = summarize_quarantines({
            "quarantine_customers": persisted["quarantine_customers"],
            "quarantine_products": persisted["quarantine_products"],
            "quarantine_orders": persisted["quarantine_orders"],
            "quarantine_order_items": persisted["quarantine_order_items"],
        })
        all_revenue = persisted["fact_sales"].agg(
            F.coalesce(F.sum("net_amount"), F.lit(0)).alias("total")
        ).first()["total"]
        completed_revenue = persisted["total_revenue"].first()["total_net_revenue"]
        revenue_summary = {
            "all_fact_revenue": str(all_revenue),
            "completed_revenue": str(completed_revenue),
            "excluded_revenue": str(all_revenue - completed_revenue),
        }
        report_path = write_quality_report(
            quality_metrics, quarantine_metrics, revenue_summary
        )
        logger.info("Wrote data quality report: %s", report_path)
        logger.info("OnlineStoreETL pipeline completed successfully")
    except Exception:
        logger.exception("OnlineStoreETL pipeline failed")
        raise
    finally:
        for df in reversed(cached):
            df.unpersist()
        spark.stop()
        logger.info("Stopped SparkSession")


def configure_local_logging() -> None:
    """Configure console logging; Airflow can capture task output."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


if __name__ == "__main__":
    configure_local_logging()
    run_pipeline()
