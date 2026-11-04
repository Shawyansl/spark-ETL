import logging
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

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


def _summarize_table(df: DataFrame) -> tuple[int, dict[str, int]]:
    expressions = [F.count(F.lit(1)).alias("_row_count")]
    expressions.extend(
        F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(column)
        for column in df.columns
    )
    row = df.agg(*expressions).first().asDict()
    null_counts = {
        column: int(row[column] or 0)
        for column in df.columns
    }
    return int(row["_row_count"] or 0), null_counts


def _find_duplicate_keys(df: DataFrame, key: str) -> list[dict[str, Any]]:
    rows = (
        df.groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .orderBy(key)
        .collect()
    )
    return [
        {
            "key": row[key],
            "rows": int(row["count"]),
            "extra_rows": int(row["count"] - 1),
        }
        for row in rows
    ]


def _find_duplicate_order_items(df: DataFrame) -> list[dict[str, Any]]:
    rows = (
        df.groupBy("order_id", "product_id")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct(
                F.struct("quantity", "unit_price", "discount_pct")
            ).alias("payload_versions"),
        )
        .filter(F.col("rows") > 1)
        .orderBy("order_id", "product_id")
        .collect()
    )
    return [
        {
            "order_id": row["order_id"],
            "product_id": row["product_id"],
            "rows": int(row["rows"]),
            "extra_rows": int(row["rows"] - 1),
            "payload_versions": int(row["payload_versions"]),
        }
        for row in rows
    ]


def _count_invalid_dates(df: DataFrame, column: str) -> int:
    parsed = F.try_to_timestamp(F.col(column), F.lit("yyyy-MM-dd"))
    return df.filter(F.col(column).isNotNull() & parsed.isNull()).count()


def _numeric_quality(df: DataFrame) -> dict[str, int]:
    quantity_wide = F.expr("try_cast(quantity AS DECIMAL(20,10))")
    quantity_int = F.expr("try_cast(quantity AS INT)")
    price_wide = F.expr("try_cast(unit_price AS DECIMAL(20,10))")
    price_rounded = F.expr("try_cast(unit_price AS DECIMAL(10,2))")
    discount_wide = F.expr("try_cast(discount_pct AS DECIMAL(20,10))")
    discount_rounded = F.expr("try_cast(discount_pct AS DECIMAL(10,4))")

    malformed = {
        "quantity": (
            F.col("quantity").isNotNull()
            & (
                quantity_wide.isNull()
                | quantity_int.isNull()
                | (quantity_wide != quantity_int.cast(DecimalType(20, 10)))
            )
        ),
        "unit_price": F.col("unit_price").isNotNull() & price_wide.isNull(),
        "discount_pct": (
            F.col("discount_pct").isNotNull() & discount_wide.isNull()
        ),
    }
    precision_loss = {
        "unit_price": (
            price_wide.isNotNull()
            & price_rounded.isNotNull()
            & (price_rounded.cast(DecimalType(20, 10)) != price_wide)
        ),
        "discount_pct": (
            discount_wide.isNotNull()
            & discount_rounded.isNotNull()
            & (discount_rounded.cast(DecimalType(20, 10)) != discount_wide)
        ),
    }
    out_of_range = {
        "unit_price": price_wide.isNotNull() & price_rounded.isNull(),
        "discount_pct": discount_wide.isNotNull() & discount_rounded.isNull(),
    }
    expressions = [
        F.sum(F.when(condition, 1).otherwise(0)).alias(f"malformed_{name}")
        for name, condition in malformed.items()
    ]
    expressions.extend(
        F.sum(F.when(condition, 1).otherwise(0)).alias(f"precision_{name}")
        for name, condition in precision_loss.items()
    )
    expressions.extend(
        F.sum(F.when(condition, 1).otherwise(0)).alias(f"out_of_range_{name}")
        for name, condition in out_of_range.items()
    )
    row = df.agg(*expressions).first().asDict()
    return {name: int(value or 0) for name, value in row.items()}


def _count_rule_violations(df: DataFrame) -> dict[str, int]:
    quantity = F.expr("try_cast(quantity AS INT)")
    unit_price = F.expr("try_cast(unit_price AS DECIMAL(10,2))")
    discount_pct = F.expr("try_cast(discount_pct AS DECIMAL(10,4))")
    rules = {
        "quantity <= 0": quantity <= 0,
        "unit_price < 0": unit_price < 0,
        "discount_pct outside [0, 1]": (discount_pct < 0) | (discount_pct > 1),
    }
    expressions = [
        F.sum(F.when(condition, 1).otherwise(0)).alias(label)
        for label, condition in rules.items()
    ]
    row = df.agg(*expressions).first().asDict()
    return {label: int(count or 0) for label, count in row.items()}


def _status_quality(df: DataFrame) -> dict[str, int]:
    normalized = F.lower(F.trim(F.col("status")))
    missing = F.col("status").isNull() | (normalized == "")
    invalid = (~normalized.isin("completed", "cancelled", "returned")) & ~missing
    row = df.agg(
        F.sum(F.when(missing, 1).otherwise(0)).alias("missing_status"),
        F.sum(F.when(invalid, 1).otherwise(0)).alias("invalid_status"),
    ).first().asDict()
    return {name: int(count or 0) for name, count in row.items()}


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
        "duplicate_order_items": [],
        "invalid_dates": {},
        "numeric_quality": {},
        "rule_violations": {},
        "status_quality": {},
        "orphan_foreign_keys": {},
    }

    for name, df in tables.items():
        row_count, null_counts = _summarize_table(df)
        metrics["row_counts"][name] = row_count
        metrics["null_counts"][name] = null_counts
        logger.info("Raw row count — %s: %d", name, row_count)
        logger.info("Null counts — %s: %s", name, null_counts)

    for name, key in KEY_COLUMNS.items():
        duplicates = _find_duplicate_keys(tables[name], key)
        metrics["duplicate_keys"][name] = {"column": key, "values": duplicates}
        logger.info("Repeated keys — %s.%s: %s", name, key, duplicates)

    metrics["duplicate_order_items"] = _find_duplicate_order_items(
        tables["order_items"]
    )
    logger.info("Repeated order-item keys: %s", metrics["duplicate_order_items"])

    for name, column in DATE_COLUMNS.items():
        count = _count_invalid_dates(tables[name], column)
        metrics["invalid_dates"][f"{name}.{column}"] = count
        logger.info("Invalid dates — %s.%s: %d rows", name, column, count)

    metrics["numeric_quality"] = _numeric_quality(tables["order_items"])
    for field, count in metrics["numeric_quality"].items():
        logger.info("Numeric quality — %s: %d rows", field, count)

    metrics["rule_violations"] = _count_rule_violations(tables["order_items"])
    for rule, count in metrics["rule_violations"].items():
        logger.info("Rule violation — %s: %d rows", rule, count)

    metrics["status_quality"] = _status_quality(tables["orders"])
    logger.info("Order status quality: %s", metrics["status_quality"])

    for child, foreign_key, parent, parent_key in FOREIGN_KEYS:
        relation = f"{child}.{foreign_key} -> {parent}.{parent_key}"
        count = _count_orphans(
            tables[child], foreign_key, tables[parent], parent_key
        )
        metrics["orphan_foreign_keys"][relation] = count
        logger.info("Orphan foreign keys — %s: %d rows", relation, count)

    return metrics


def summarize_quarantines(
    quarantines: dict[str, DataFrame],
) -> dict[str, dict[str, int]]:
    """Return rejected-row counts grouped by their primary reason."""
    summaries: dict[str, dict[str, int]] = {}
    for name, df in quarantines.items():
        reason_counts = {
            row["reject_reason"]: int(row["count"])
            for row in df.groupBy("reject_reason").count().collect()
        }
        summaries[name] = reason_counts
        logger.info("Quarantine counts — %s: %s", name, reason_counts)
    return summaries


def write_quality_report(
    metrics: dict[str, Any],
    quarantine_metrics: dict[str, dict[str, int]] | None = None,
    revenue_summary: dict[str, Any] | None = None,
) -> Path:
    """Write raw quality, post-cleaning rejects, and revenue reconciliation."""
    lines = [
        "# Data Quality and Reconciliation Report",
        "",
        "Raw-input metrics are measured before cleaning. Rejected-row counts and revenue reconciliation are measured after the ETL transformations.",
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
        "## Duplicate order-item keys",
        "",
        "The intended order-line key is `(order_id, product_id)`. `payload_versions` shows whether repeated keys carry identical or conflicting quantity/price/discount values.",
        "",
        "| order_id | product_id | Rows | Extra rows | Payload versions |",
        "|---:|---:|---:|---:|---:|",
    ])
    lines.extend(
        f"| {item['order_id']} | {item['product_id']} | {item['rows']} | "
        f"{item['extra_rows']} | {item['payload_versions']} |"
        for item in metrics["duplicate_order_items"]
    )
    if not metrics["duplicate_order_items"]:
        lines.append("| — | — | 0 | 0 | 0 |")

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
        "## Malformed, over-precision, and out-of-range numeric values",
        "",
        "| Check | Rows |",
        "|---|---:|",
    ])
    lines.extend(
        f"| `{check}` | {count} |"
        for check, count in metrics["numeric_quality"].items()
    )

    lines.extend([
        "",
        "## Order-item rule violations",
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
        "## Order-status quality",
        "",
        "| Status check | Rows |",
        "|---|---:|",
    ])
    lines.extend(
        f"| `{name}` | {count} |"
        for name, count in metrics["status_quality"].items()
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

    if quarantine_metrics is not None:
        lines.extend([
            "",
            "## Rejected rows after cleaning",
            "",
            "Each row is assigned its first matching rejection reason. A row with multiple defects appears once under the highest-priority reason selected by the cleaning rules.",
            "",
            "| Quarantine dataset | Reject reason | Rows |",
            "|---|---|---:|",
        ])
        for dataset, reasons in quarantine_metrics.items():
            if reasons:
                lines.extend(
                    f"| `{dataset}` | `{reason}` | {count} |"
                    for reason, count in sorted(reasons.items())
                )
            else:
                lines.append(f"| `{dataset}` | — | 0 |")

    if revenue_summary is not None:
        lines.extend([
            "",
            "## Revenue reconciliation",
            "",
            "| Measure | Net amount | Explanation |",
            "|---|---:|---|",
            f"| All valid fact rows | {revenue_summary['all_fact_revenue']} | Includes valid non-completed orders |",
            f"| Completed orders | {revenue_summary['completed_revenue']} | Revenue used by the business reports |",
            f"| Excluded status difference | {revenue_summary['excluded_revenue']} | Non-completed valid fact rows excluded from reports |",
        ])

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("Wrote data quality report — %s", REPORT_PATH)
    return REPORT_PATH
