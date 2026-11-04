from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

MONEY = DecimalType(18, 2)


def add_amounts(fact_sales: DataFrame) -> DataFrame:
    """Calculate line amounts, rounding discounts to cents per line."""
    gross = (F.col("quantity") * F.col("unit_price")).cast(MONEY)
    discount = F.round(gross * F.col("discount_pct"), 2).cast(MONEY)
    return (
        fact_sales
        .withColumn("gross_amount", gross)
        .withColumn("discount_amount", discount)
        .withColumn("net_amount", (gross - discount).cast(MONEY))
    )


def build_reports(sales_with_amounts: DataFrame) -> dict[str, DataFrame]:
    """Aggregate completed orders only, regardless of caller input."""
    completed = sales_with_amounts.filter(F.col("status") == "completed")
    return {
        "total_revenue": completed.agg(
            F.coalesce(F.sum("net_amount"), F.lit(0).cast(MONEY))
            .cast(MONEY).alias("total_net_revenue")
        ),
        "daily_revenue": (
            completed.groupBy("order_date")
            .agg(F.sum("net_amount").cast(MONEY).alias("net_revenue"))
            .orderBy("order_date")
        ),
        "category_revenue": (
            completed.groupBy("category")
            .agg(F.sum("net_amount").cast(MONEY).alias("net_revenue"))
            .orderBy("category")
        ),
        "city_revenue": (
            completed.groupBy("city")
            .agg(F.sum("net_amount").cast(MONEY).alias("net_revenue"))
            .orderBy("city")
        ),
        "completed_orders_per_day": (
            completed.groupBy("order_date")
            .agg(F.countDistinct("order_id").alias("completed_orders"))
            .orderBy("order_date")
        ),
        "top_customers": (
            completed.groupBy("customer_id", "full_name")
            .agg(F.sum("net_amount").cast(MONEY).alias("net_revenue"))
            .orderBy(F.col("net_revenue").desc(), F.col("customer_id").asc())
            .limit(5)
        ),
    }
