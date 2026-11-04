from pyspark.sql import DataFrame

FACT_COLUMNS = [
    "order_id", "order_date", "customer_id", "full_name", "city", "segment",
    "product_id", "product_name", "category", "quantity", "unit_price",
    "discount_pct", "status",
]


def build_fact_sales(
    cleaned_orders: DataFrame,
    cleaned_customers: DataFrame,
    cleaned_order_items: DataFrame,
    cleaned_products: DataFrame,
) -> DataFrame:
    """Build one fact row per cleaned (order_id, product_id) order line.

    Inner joins are safe after reference validation: they exclude invalid
    relationships, while unique dimension keys and the order-line key prevent
    join fanout. The row-count guard detects an unexpected loss or multiplication.
    """
    customer_info = cleaned_customers.select(
        "customer_id", "full_name", "city", "segment"
    )
    product_info = cleaned_products.select(
        "product_id", "product_name", "category"
    )
    orders_with_customers = cleaned_orders.join(
        customer_info, on="customer_id", how="inner"
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
            f"Join changed row count: {item_count} cleaned order lines "
            f"became {fact_count} fact rows"
        )
    return fact_sales
