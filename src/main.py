import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.cleaning import (
    clean_customers,
    clean_order_items,
    clean_orders,
    clean_products,
)
from src.data_quality import analyze_data_quality, write_quality_report
from src.joins import build_fact_sales
from src.read_data import load_data
from src.sales_report import add_amounts, build_reports
from src.write_outputs import write_outputs
from src.validate_outputs import (
    validate_outputs,
    validate_unique_customer_ids,
    validate_sales_values,
    validate_discount_range,
    validate_completed_sales_only,
    validate_sales_references,
    validate_revenue_reconciliation,
)
logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    """Run the full local ETL workflow from raw CSVs to Parquet outputs."""
    spark = (
        SparkSession.builder
        .appName("OnlineStoreETL")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Starting OnlineStoreETL pipeline")

        # 1. Ingest raw CSV data and record the pre-cleaning quality report.
        tables = load_data(spark)
        for name, df in tables.items():
            logger.info("Raw schema — %s", name)
            df.printSchema()

        quality_metrics = analyze_data_quality(tables)
        quality_report_path = write_quality_report(quality_metrics)
        logger.info("Wrote raw data quality report: %s", quality_report_path)

        # 2. Clean dimensions and transactions, keeping invalid records in quarantine.
        cleaned_customers = clean_customers(tables["customers"])
        validate_unique_customer_ids(cleaned_customers)
        cleaned_products = clean_products(tables["products"])
        cleaned_orders, quarantine_orders, duplicate_orders_removed = clean_orders(
            tables["orders"], cleaned_customers
        )
        cleaned_order_items, quarantine_order_items = clean_order_items(
            tables["order_items"], cleaned_orders, cleaned_products
        )
        logger.info("Removed duplicate orders: %d", duplicate_orders_removed)
        logger.info("Cleaned order item rows: %d", cleaned_order_items.count())
        logger.info("Quarantined order item rows: %d", quarantine_order_items.count())

        # 3. Build the sales fact and calculate line-level monetary amounts.
        fact_sales = build_fact_sales(
            cleaned_orders,
            cleaned_customers,
            cleaned_order_items,
            cleaned_products,
        )
        
        validate_sales_references(
            fact_sales,
            cleaned_customers,
            cleaned_products,
        )   

        validate_sales_values(fact_sales)
        validate_discount_range(fact_sales)

        sales_with_amounts = add_amounts(fact_sales)

        # 4. All revenue reports are based only on completed orders.
        completed_sales = sales_with_amounts.filter(
            F.col("status") == "completed"
        )
        validate_completed_sales_only(completed_sales)
        reports = build_reports(completed_sales)
        validate_revenue_reconciliation(
            completed_sales,
            reports["total_revenue"],
        )

        # 5. Persist cleaned data, quarantine data, the fact table, and reports.
        outputs = {
            "cleaned_customers": cleaned_customers,
            "cleaned_products": cleaned_products,
            "cleaned_orders": cleaned_orders,
            "cleaned_order_items": cleaned_order_items,
            "quarantine_orders": quarantine_orders,
            "quarantine_order_items": quarantine_order_items,
            "fact_sales": sales_with_amounts,
            **reports,
        }
        write_outputs(outputs)
        validate_outputs(spark, outputs)
        logger.info("OnlineStoreETL pipeline completed successfully")
    except Exception:
        logger.exception("OnlineStoreETL pipeline failed")
        raise
    finally:
        spark.stop()
        logger.info("Stopped SparkSession")


def configure_local_logging() -> None:
    """Configure console logging for local runs; Airflow captures task logs."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


if __name__ == "__main__":
    configure_local_logging()
    run_pipeline()
