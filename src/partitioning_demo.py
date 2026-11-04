from pyspark.sql import SparkSession
from src.cleaning import (
    clean_customers,
    clean_order_items,
    clean_orders,
    clean_products,
)
from src.joins import build_fact_sales
from src.read_data import load_data
from src.sales_report import add_amounts, build_reports


def run_partition_demo() -> None:
    spark = (
        SparkSession.builder
        .appName("PartitioningDemo")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        spark.sparkContext.setLogLevel("ERROR")

        # Build the same completed-sales daily revenue report used by the pipeline.
        tables = load_data(spark)
        cleaned_customers = clean_customers(tables["customers"])
        cleaned_products = clean_products(tables["products"])
        cleaned_orders, quarantine_orders = clean_orders(
            tables["orders"],
            cleaned_customers,
        )
        cleaned_order_items, _ = clean_order_items(
            tables["order_items"],
            cleaned_orders,
            cleaned_products,
            quarantine_orders,
        )

        fact_sales = build_fact_sales(
            cleaned_orders,
            cleaned_customers,
            cleaned_order_items,
            cleaned_products,
        )
        sales_with_amounts = add_amounts(fact_sales)
        daily_revenue = build_reports(sales_with_amounts)["daily_revenue"]

        # Inspect the physical plan for this aggregation report.
        print("\nExecution plan for daily_revenue:")
        daily_revenue.explain("formatted")

        print(f"\nInitial partitions: {daily_revenue.rdd.getNumPartitions()}")

        repartitioned_df = daily_revenue.repartition(4)
        print(
            "After repartition(4): "
            f"{repartitioned_df.rdd.getNumPartitions()} partitions"
        )
        print(f"Rows after repartition: {repartitioned_df.count()}")
        print("\nPlan after repartition(4):")
        repartitioned_df.explain("formatted")

        coalesced_df = repartitioned_df.coalesce(2)
        print(
            "\nAfter coalesce(2): "
            f"{coalesced_df.rdd.getNumPartitions()} partitions"
        )
        print(f"Rows after coalesce: {coalesced_df.count()}")
        print("\nPlan after coalesce(2):")
        coalesced_df.explain("formatted")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_partition_demo()
