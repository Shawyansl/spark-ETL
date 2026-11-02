from pyspark.sql import DataFrame


FACT_COLUMNS = [
    "order_id",
    "order_date",
    "customer_id",
    "full_name",
    "city",
    "segment",
    "product_id",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "discount_pct",
    "status",
]


def build_fact_sales(cleaned_orders: DataFrame, cleaned_customers: DataFrame, cleaned_order_items: DataFrame, cleaned_products: DataFrame) -> DataFrame:
    """join the cleaned tables to produce the fact_sales table."""
    customer_info = cleaned_customers.select(
        "customer_id", "full_name", "city", "segment"
    )
    product_info = cleaned_products.select(
        "product_id", "product_name", "category"
    )

    orders_with_customers = cleaned_orders.join(
        customer_info,
        on="customer_id",
        how="inner",
    )

    fact_sales = (
        cleaned_order_items
        .join(orders_with_customers, on="order_id", how="inner")
        .join(product_info, on="product_id", how="inner")
        .select(*FACT_COLUMNS)
    )

    item_count = cleaned_order_items.count()
    fact_count = fact_sales.count()

    if fact_count != item_count:
        raise ValueError(
            f"Join changed the row count: {item_count} items became "
            f"{fact_count} fact rows"
        )

    return fact_sales

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from src.read_data import load_data
    from src.cleaning import clean_orders, clean_order_items, clean_customers, clean_products

    spark = SparkSession.builder.getOrCreate()
    tables = load_data(spark)

    cleaned_customers = clean_customers(tables["customers"])
    cleaned_products = clean_products(tables["products"])
    cleaned_orders, quarantine_orders, duplicates_removed_orders = clean_orders(tables["orders"], cleaned_customers)
    cleaned_order_items, quarantine_order_items = clean_order_items(
        tables["order_items"], cleaned_orders, cleaned_products
    )
    fact_sales = build_fact_sales(cleaned_orders, cleaned_customers, cleaned_order_items, cleaned_products)
    fact_sales.show()