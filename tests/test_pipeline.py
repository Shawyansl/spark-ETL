import os
import unittest
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType, IntegerType, StringType, StructField, StructType,
)

from src.cleaning import clean_order_items, clean_orders
from src.data_quality import _numeric_quality
from src.sales_report import add_amounts, build_reports


class PipelineRulesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("OnlineStoreETLTests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_order_status_normalization_and_quarantine(self):
        schema = StructType([
            StructField("order_id", IntegerType()),
            StructField("customer_id", IntegerType()),
            StructField("order_date", StringType()),
            StructField("status", StringType()),
            StructField("payment_method", StringType()),
        ])
        orders = self.spark.createDataFrame([
            (1, 10, "2024-01-01", " Completed ", "card"),
            (2, 10, "2024-01-02", "shipped", "card"),
            (3, 10, "2024-01-03", "   ", "card"),
        ], schema)
        customers = self.spark.createDataFrame([(10,)], ["customer_id"])
        cleaned, quarantine = clean_orders(orders, customers)
        self.assertEqual([(1, "completed")], [
            (r.order_id, r.status) for r in cleaned.select("order_id", "status").collect()
        ])
        self.assertEqual(
            {"invalid_status", "missing_status"},
            {r.reject_reason for r in quarantine.select("reject_reason").collect()},
        )

    def test_conflicting_orders_are_all_quarantined(self):
        schema = "order_id int, customer_id int, order_date string, status string, payment_method string"
        orders = self.spark.createDataFrame([
            (7, 10, "2024-01-01", "completed", "card"),
            (7, 10, "2024-01-01", "completed", "cash"),
        ], schema)
        customers = self.spark.createDataFrame([(10,)], ["customer_id"])
        cleaned, quarantine = clean_orders(orders, customers)
        self.assertEqual(0, cleaned.count())
        self.assertEqual(2, quarantine.filter(
            "reject_reason = 'conflicting_duplicate_order'"
        ).count())

    def test_order_item_duplicate_and_invalid_reasons(self):
        item_schema = "order_id int, product_id int, quantity string, unit_price string, discount_pct string"
        items = self.spark.createDataFrame([
            (1, 10, "1", "10.00", "0.00"),
            (1, 10, "1", "10.00", "0.00"),
            (1, 13, "1", "10.00", "0.00"),
            (1, 13, "2", "10.00", "0.00"),
            (1, 11, "1", "10.001", "0.00"),
            (1, 12, "0", "10.00", "0.00"),
            (1, 14, "1", "100000000.00", "0.00"),
            (1, 15, "1", "10.00", "1000000.0000"),
            (2, 10, "1", "10.00", "0.00"),
            (99, 10, "1", "10.00", "0.00"),
        ], item_schema)
        orders = self.spark.createDataFrame([(1,)], ["order_id"])
        products = self.spark.createDataFrame(
            [(10,), (11,), (12,), (13,), (14,), (15,)], ["product_id"]
        )
        quarantine_orders = self.spark.createDataFrame([
            (2, "orphan_customer",)
        ], "order_id int, reject_reason string")
        cleaned, quarantine = clean_order_items(
            items, orders, products, quarantine_orders
        )
        quality = _numeric_quality(items)
        self.assertEqual(1, quality["out_of_range_unit_price"])
        self.assertEqual(1, quality["out_of_range_discount_pct"])
        self.assertEqual(1, cleaned.count())
        reasons = {
            row.reject_reason: row["count"]
            for row in quarantine.groupBy("reject_reason").count().collect()
        }
        self.assertEqual(1, reasons["duplicate_order_item"])
        self.assertEqual(2, reasons["conflicting_duplicate_order_item"])
        self.assertEqual(1, reasons["invalid_unit_price_precision"])
        self.assertEqual(1, reasons["invalid_unit_price"])
        self.assertEqual(1, reasons["invalid_discount_pct"])
        self.assertEqual(1, reasons["invalid_quantity"])
        self.assertEqual(1, reasons["order_quarantined"])
        self.assertEqual(1, reasons["orphan_order"])

    def test_amounts_round_to_cents_and_reports_filter_status(self):
        schema = StructType([
            StructField("order_id", IntegerType()),
            StructField("order_date", StringType()),
            StructField("customer_id", IntegerType()),
            StructField("full_name", StringType()),
            StructField("city", StringType()),
            StructField("category", StringType()),
            StructField("status", StringType()),
            StructField("quantity", IntegerType()),
            StructField("unit_price", DecimalType(10, 2)),
            StructField("discount_pct", DecimalType(10, 4)),
        ])
        rows = [
            (1, "2024-01-01", 10, "A", "X", "C", "completed", 1, Decimal("19.99"), Decimal("0.1000")),
            (2, "2024-01-01", 11, "B", "Y", "D", "cancelled", 1, Decimal("100.00"), Decimal("0.0000")),
        ]
        sales = add_amounts(self.spark.createDataFrame(rows, schema))
        completed = sales.filter("order_id = 1").first()
        self.assertEqual(Decimal("2.00"), completed.discount_amount)
        self.assertEqual(Decimal("17.99"), completed.net_amount)
        reports = build_reports(sales)
        self.assertEqual(Decimal("17.99"), reports["total_revenue"].first().total_net_revenue)


if __name__ == "__main__":
    unittest.main()
