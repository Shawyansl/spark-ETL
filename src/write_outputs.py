import logging
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


logger = logging.getLogger(__name__)
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"


def write_outputs(outputs: dict[str, DataFrame]) -> None:
    for name, df in outputs.items():
        path = OUTPUT_DIR / name

        if name == "fact_sales":
            df = df.withColumn(
                "order_month",
                F.date_format("order_date", "yyyy-MM"),
            )
            df.write.mode("overwrite").partitionBy("order_month").parquet(
                str(path)
            )
        else:
            df.write.mode("overwrite").parquet(str(path))

        logger.info("Wrote Parquet output: %s", path)