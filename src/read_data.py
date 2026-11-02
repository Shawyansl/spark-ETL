from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# Keep raw date values as strings so malformed values remain visible to quality checks.
CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("full_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("segment", StringType(), True),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(10, 2), True),
    StructField("discount_pct", DecimalType(10, 4), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
])


def read_csv(spark: SparkSession, file_name: str, schema: StructType) -> DataFrame:
    """Read one raw CSV using its explicit schema."""
    return (
        spark.read
        .option("header", True)
        .option("nullValue", "NULL")
        .schema(schema)
        .csv(str(DATA_DIR / file_name))
    )


def load_data(spark: SparkSession) -> dict[str, DataFrame]:
    """Load all raw input tables without transforming or cleaning them."""
    return {
        "customers": read_csv(spark, "customers.csv", CUSTOMERS_SCHEMA),
        "orders": read_csv(spark, "orders.csv", ORDERS_SCHEMA),
        "order_items": read_csv(spark, "order_items.csv", ORDER_ITEMS_SCHEMA),
        "products": read_csv(spark, "products.csv", PRODUCTS_SCHEMA),
    }
