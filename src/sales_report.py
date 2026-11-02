from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_amounts(fact_sales: DataFrame) -> DataFrame:
    return (
        fact_sales
        .withColumn(
            "gross_amount",
            F.col("quantity") * F.col("unit_price"),
        )
        .withColumn(
            "discount_amount",
            F.col("gross_amount") * F.col("discount_pct"),
        )
        .withColumn(
            "net_amount",
            F.col("gross_amount") - F.col("discount_amount"),
        )
    )

def build_reports(completed_sales: DataFrame) -> dict[str, DataFrame]:
    return {
        "total_revenue": completed_sales.agg(
            F.sum("net_amount").alias("total_net_revenue")
        ),

        "daily_revenue": (
            completed_sales
            .groupBy("order_date")
            .agg(F.sum("net_amount").alias("net_revenue"))
            .orderBy("order_date")
        ),

        "category_revenue": (
            completed_sales
            .groupBy("category")
            .agg(F.sum("net_amount").alias("net_revenue"))
            .orderBy("category")
        ),

        "city_revenue": (
            completed_sales
            .groupBy("city")
            .agg(F.sum("net_amount").alias("net_revenue"))
            .orderBy("city")
        ),

        "completed_orders_per_day": (
            completed_sales
            .groupBy("order_date")
            .agg(F.countDistinct("order_id").alias("completed_orders"))
            .orderBy("order_date")
        ),

        "top_customers": (
            completed_sales
            .groupBy("customer_id", "full_name")
            .agg(F.sum("net_amount").alias("net_revenue"))
            .orderBy(
                F.col("net_revenue").desc(),
                F.col("customer_id").asc(),
            )
            .limit(5)
        ),
    }


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from src.read_data import load_data
    from src.cleaning import clean_orders, clean_order_items, clean_customers, clean_products
    from src.joins import build_fact_sales

    spark = SparkSession.builder.getOrCreate()
    tables = load_data(spark)

    cleaned_customers = clean_customers(tables["customers"])
    cleaned_products = clean_products(tables["products"])
    cleaned_orders, quarantine_orders, duplicates_removed_orders = clean_orders(tables["orders"], cleaned_customers)
    cleaned_order_items, quarantine_order_items = clean_order_items(
        tables["order_items"], cleaned_orders, cleaned_products
    )

    fact_sales = build_fact_sales(
        cleaned_orders, cleaned_customers, cleaned_order_items, cleaned_products
    )
    sales_with_amounts = add_amounts(fact_sales)

    completed_sales = sales_with_amounts.filter(
        F.col("status") == "completed"
    )

    reports = build_reports(completed_sales)
    for report_name, report_df in reports.items():
        print(f"\n{report_name.upper()}:")
        report_df.show(truncate=False)

    from src.write_outputs import write_outputs

    outputs = {
        "cleaned_customers": cleaned_customers,
        "cleaned_products": cleaned_products,
        "cleaned_orders": cleaned_orders,
        "quarantine_orders": quarantine_orders,
        "quarantine_order_items": quarantine_order_items,
        "fact_sales": sales_with_amounts,
        **reports,
    }
    
    write_outputs(outputs)