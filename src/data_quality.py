import logging
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
REPORT_PATH = PROJECT_ROOT / "reports" / "data_quality_report.md"

KEY_COLUMNS = {
    "customers": "customer_id",
    "orders": "order_id",
    "products": "product_id",
}
DATE_COLUMNS = {
    "customers": "signup_date",
    "orders": "order_date",
}
FOREIGN_KEYS = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
]


def _count_nulls(df: DataFrame) -> dict[str, int]:
    expressions = [
        F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(column)
        for column in df.columns
    ]
    row = df.agg(*expressions).first().asDict()
    return {column: int(count or 0) for column, count in row.items()}


def _find_duplicate_keys(df: DataFrame, key: str) -> list[dict[str, Any]]:
    rows = (
        df.filter(F.col(key).isNotNull())
        .groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .orderBy(key)
        .collect()
    )
    return [
        {"key": row[key], "rows": int(row["count"]), "extra_rows": int(row["count"] - 1)}
        for row in rows
    ]


def _count_invalid_dates(df: DataFrame, column: str) -> int:
    parsed = F.try_to_timestamp(F.col(column), F.lit("yyyy-MM-dd"))
    return df.filter(F.col(column).isNotNull() & parsed.isNull()).count()


def _count_rule_violations(df: DataFrame) -> dict[str, int]:
    rules = {
        "quantity <= 0": F.col("quantity") <= 0,
        "unit_price < 0": F.col("unit_price") < 0,
        "discount_pct outside [0, 1]": (
            (F.col("discount_pct") < 0) | (F.col("discount_pct") > 1)
        ),
    }
    return {label: df.filter(condition).count() for label, condition in rules.items()}


def _count_orphans(
    child_df: DataFrame,
    foreign_key: str,
    parent_df: DataFrame,
    parent_key: str,
) -> int:
    child_non_null = child_df.filter(F.col(foreign_key).isNotNull())
    parent_keys = parent_df.select(F.col(parent_key).alias("_parent_key")).distinct()
    return child_non_null.join(
        parent_keys,
        child_non_null[foreign_key] == parent_keys["_parent_key"],
        "left_anti",
    ).count()


def analyze_data_quality(tables: dict[str, DataFrame]) -> dict[str, Any]:
    """Measure data quality on raw inputs and log each result."""
    metrics: dict[str, Any] = {
        "row_counts": {},
        "null_counts": {},
        "duplicate_keys": {},
        "invalid_dates": {},
        "rule_violations": {},
        "orphan_foreign_keys": {},
    }

    """for each table, compute row counts and null counts for all columns."""
    for name, df in tables.items():
        metrics["row_counts"][name] = df.count()
        metrics["null_counts"][name] = _count_nulls(df)
        logger.info("Raw row count — %s: %d", name, metrics["row_counts"][name])
        logger.info("Null counts — %s: %s", name, metrics["null_counts"][name])

    """for each table with a key column, find duplicate keys and log the results."""
    for name, key in KEY_COLUMNS.items():
        duplicates = _find_duplicate_keys(tables[name], key)
        metrics["duplicate_keys"][name] = {"column": key, "values": duplicates}
        logger.info("Repeated keys — %s.%s: %s", name, key, duplicates)

    """for each table with a date column, count invalid dates and log the results."""
    for name, column in DATE_COLUMNS.items():
        count = _count_invalid_dates(tables[name], column)
        metrics["invalid_dates"][f"{name}.{column}"] = count
        logger.info("Invalid dates — %s.%s: %d rows", name, column, count)

    """for the order_items table, count rule violations and log the results."""
    metrics["rule_violations"] = _count_rule_violations(tables["order_items"])
    for rule, count in metrics["rule_violations"].items():
        logger.info("Rule violation — %s: %d rows", rule, count)

    """for each foreign key relationship, count orphan rows and log the results."""
    for child, foreign_key, parent, parent_key in FOREIGN_KEYS:
        relation = f"{child}.{foreign_key} -> {parent}.{parent_key}"
        count = _count_orphans(
            tables[child], foreign_key, tables[parent], parent_key
        )
        metrics["orphan_foreign_keys"][relation] = count
        logger.info("Orphan foreign keys — %s: %d rows", relation, count)

    return metrics


def write_quality_report(metrics: dict[str, Any]) -> Path:
    """Write a Markdown snapshot of the raw-data quality metrics."""
    lines = [
        "# Raw Data Quality Report",
        "",
        "This report describes the input data before cleaning.",
        "",
        "## Row counts",
        "",
        "| Table | Rows |",
        "|---|---:|",
    ]
    lines.extend(
        f"| `{table}` | {count} |"
        for table, count in metrics["row_counts"].items()
    )

    lines.extend([
        "",
        "## Null counts",
        "",
        "| Table | Column | Null rows |",
        "|---|---|---:|",
    ])
    for table, columns in metrics["null_counts"].items():
        lines.extend(
            f"| `{table}` | `{column}` | {count} |"
            for column, count in columns.items()
        )

    lines.extend([
        "",
        "## Duplicate keys",
        "",
        "| Table | Key column | Key value | Rows with key | Extra rows |",
        "|---|---|---:|---:|---:|",
    ])
    for table, result in metrics["duplicate_keys"].items():
        if result["values"]:
            lines.extend(
                f"| `{table}` | `{result['column']}` | `{item['key']}` | "
                f"{item['rows']} | {item['extra_rows']} |"
                for item in result["values"]
            )
        else:
            lines.append(f"| `{table}` | `{result['column']}` | — | 0 | 0 |")

    lines.extend([
        "",
        "## Invalid dates",
        "",
        "| Table and column | Invalid rows |",
        "|---|---:|",
    ])
    lines.extend(
        f"| `{column}` | {count} |"
        for column, count in metrics["invalid_dates"].items()
    )

    lines.extend([
        "",
        "## Order item rule violations",
        "",
        "| Rule | Rows |",
        "|---|---:|",
    ])
    lines.extend(
        f"| {rule} | {count} |"
        for rule, count in metrics["rule_violations"].items()
    )

    lines.extend([
        "",
        "## Orphan foreign keys",
        "",
        "| Relationship | Rows without a parent match |",
        "|---|---:|",
    ])
    lines.extend(
        f"| `{relation}` | {count} |"
        for relation, count in metrics["orphan_foreign_keys"].items()
    )

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("Wrote raw data quality report — %s", REPORT_PATH)
    return REPORT_PATH
