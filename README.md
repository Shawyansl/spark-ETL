# Online Store ETL — PySpark

A small, end-to-end batch ETL project built with PySpark. It reads four CSV files for an online store, profiles the raw data, applies explicit cleaning and quarantine rules, builds a line-level sales fact, calculates revenue reports for completed orders, writes Parquet datasets, and validates the persisted outputs.

This README is intended to be a practical handoff: it describes the current implementation, how to run it, what results to expect from the supplied sample data, and what to revisit before extending the project.

> **Current scope:** local batch processing for CSV inputs, with a modular Python implementation and automated rule tests. There is no Airflow DAG, cloud deployment, database sink, or incremental processing.

## Contents

- [What the pipeline does](#what-the-pipeline-does)
- [Repository layout](#repository-layout)
- [Requirements and setup](#requirements-and-setup)
- [Run the project](#run-the-project)
- [Data contract](#data-contract)
- [Pipeline stages and business rules](#pipeline-stages-and-business-rules)
- [Outputs](#outputs)
- [Validation and expected sample results](#validation-and-expected-sample-results)
- [Logging and troubleshooting](#logging-and-troubleshooting)
- [Design notes and current limitations](#design-notes-and-current-limitations)
- [Suggested next steps](#suggested-next-steps)

## What the pipeline does

```mermaid
flowchart LR
    A[data/*.csv] --> B[Read with explicit schemas]
    B --> C[Raw data quality profile]
    C --> D[Clean dimensions and orders]
    D --> E[Quarantine invalid records]
    D --> F[Join valid records into fact_sales]
    F --> G[Calculate gross, discount, and net amounts]
    G --> H[Filter status = completed]
    H --> I[Build six revenue reports]
    D --> J[Write cleaned and quarantine datasets]
    G --> K[Write fact_sales]
    I --> L[Write report datasets]
    J --> M[Read-after-write validation]
    K --> M
    L --> M
```

`src/main.py` is the orchestration entry point. The pipeline uses a local Spark session (`local[*]`), so Spark runs on the machine that launches the command and can use its available local cores.

## Repository layout

```text
pyspark_etl_homework/
├── README.md
├── requirements.txt
├── data/                         # Input CSV fixtures
├── output/                       # Regenerated local Parquet outputs (ignored by Git)
├── sample_output/                # Preserved example Parquet artifacts
├── reports/
│   └── data_quality_report.md    # Raw quality, quarantine counts, and reconciliation
├── src/
│   ├── main.py                   # End-to-end orchestration and console logging
│   ├── read_data.py              # Explicit schemas and CSV ingestion
│   ├── data_quality.py           # Raw data profiling and Markdown report
│   ├── cleaning.py               # Cleaning, deduplication, and quarantine rules
│   ├── joins.py                  # Fact table construction and row-count guard
│   ├── sales_report.py           # Amount calculations and business aggregations
│   ├── write_outputs.py          # Parquet persistence
│   ├── validate_outputs.py       # Business and persisted-output checks
│   ├── partitioning_demo.py      # repartition/coalesce/explain learning exercise
│   ├── generate_data.py          # Recreates the sample CSV fixtures
│   └── spark_test.py             # Minimal Spark startup/version smoke check
└── tests/
    └── test_pipeline.py          # Business-rule tests (unittest)
```

## Requirements and setup

The source uses Python, PySpark, and Java. The project was run with Python 3.12, PySpark 4.2.1, and OpenJDK 21. The PySpark dependency is pinned in `requirements.txt`; check that a compatible Java runtime is available on `PATH` before launching Spark. From the repository root, create an isolated environment and install PySpark:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Check the installed Spark version and that a local session starts:

```bash
python -m src.spark_test
```

If you already use the virtual environment in the parent directory, the equivalent invocation used during development is:

```bash
../.venv/bin/python -m src.main
```

Run the commands from the project root so Python can import the `src` package. Input, report, and output paths are derived from the source-file location, so the CSV inputs do not depend on the shell's current directory. There is intentionally no notebook: each ETL stage has a focused module, while `src.main` gives one reproducible orchestration path and `tests/` captures the core rules.

## Run the project

Run the complete ETL pipeline:

```bash
python -m src.main
```

Run the automated business-rule tests:

```bash
python -m unittest discover -s tests -v
```

Run the partitioning exercise separately:

```bash
python -m src.partitioning_demo
```

The demo builds the daily completed-sales report, prints its initial partition count and physical plan, applies `repartition(4)`, then applies `coalesce(2)`, and prints the resulting counts and plans. `repartition` redistributes rows through a shuffle and can increase or decrease partitions; `coalesce` usually reduces partitions by combining existing partitions with less data movement. Joins and `groupBy` may shuffle for key-based redistribution. Adaptive Query Execution can coalesce shuffle partitions at runtime, so observed physical plans may differ from the initial plan. A repartition after `orderBy` does not preserve global ordering. It is a learning exercise over a tiny aggregate; its partition counts are not a production tuning recommendation.

To recreate the CSV fixtures:

```bash
python -m src.generate_data
```

**Caution:** this command overwrites all four files in `data/` with the built-in sample dataset. Back up or commit any input changes before running it.

A successful `src.main` run overwrites datasets under `output/`, reads the Parquet outputs back, validates their required columns and business rules, then writes `reports/data_quality_report.md` with raw quality, quarantine reasons, and revenue reconciliation. Generated `output/` is ignored by Git; `sample_output/` preserves example artifacts. A failed validation raises an exception and the process exits unsuccessfully.

## Data contract

The pipeline expects these CSV files with header rows. The reader uses explicitly declared schemas; it does not rely on `inferSchema`.

| File | Column | CSV schema type | Cleaned representation |
|---|---|---|---|
| `customers.csv` | `customer_id` | integer | integer |
|  | `full_name` | string | string |
|  | `city` | string | string (`NULL` becomes `Unknown`) |
|  | `signup_date` | string | date; malformed values become null |
|  | `segment` | string | string |
| `orders.csv` | `order_id` | integer | integer |
|  | `customer_id` | integer | integer |
|  | `order_date` | string | date; invalid/missing values are quarantined |
|  | `status` | string | string |
|  | `payment_method` | string | string |
| `order_items.csv` | `order_id` | integer | integer |
|  | `product_id` | integer | integer |
|  | `quantity` | string (raw text) | integer |
|  | `unit_price` | string (raw text) | `DECIMAL(10,2)` |
|  | `discount_pct` | string (raw text) | `DECIMAL(10,4)` |
| `products.csv` | `product_id` | integer | integer |
|  | `product_name` | string | string |
|  | `category` | string | string |

`header=True` is enabled and the literal text `NULL` is interpreted as a null value. The reader declares every schema field as nullable; the cleaning rules decide which nulls are replaced, retained, or quarantined. Date columns are initially read as strings on purpose: malformed source values remain available for the raw quality report, then are parsed during cleaning with the `yyyy-MM-dd` format.

The expected relationships are:

- `orders.customer_id` → `customers.customer_id`
- `order_items.order_id` → `orders.order_id`
- `order_items.product_id` → `products.product_id`

The reader currently points to the four local files under `data/`. There is no external configuration layer for choosing another source path or environment.

## Pipeline stages and business rules

### 1. Ingest and inspect raw data

`src/read_data.py` loads the four CSVs using explicit schemas. `src/main.py` prints each raw schema with `printSchema()` and starts the raw data-quality analysis before cleaning changes the evidence.

### 2. Profile raw data quality

`src/data_quality.py` logs and writes a Markdown snapshot containing:

- Row and per-column null counts.
- Duplicate values for the configured customer, order, and product keys.
- Malformed non-null dates.
- Malformed numeric values, precision loss, numeric overflow, and order-item quantity, price, and discount violations.
- Non-null foreign keys without a matching parent row.

The report is a **pre-cleaning snapshot**. Null foreign keys are visible in null counts and are not counted as orphan references. The report is regenerated on each run, so preserve or version it separately if historical run-by-run metrics are needed.

### 3. Clean dimensions and orders

`src/cleaning.py` applies these current rules:

| Dataset | Rule |
|---|---|
| Customers | Drop null `customer_id` into `quarantine_customers`; deduplicate by key; trim city and replace null/blank with `Unknown`; parse `signup_date`. An invalid signup date becomes null but the customer remains. |
| Products | Drop null `product_id` into `quarantine_products`; deduplicate by key. |
| Orders | Normalize status by trimming/lowercasing; accept only `completed`, `cancelled`, and `returned`. Parse `order_date`; quarantine missing keys, invalid dates/statuses, and unknown customers. Exact repeated source rows are quarantined as `duplicate_order`; conflicting versions of one order ID are all quarantined. |

`cancelled` and `returned` order statuses remain in `cleaned_orders`. Status determines whether a sale contributes to revenue; it is not an order-cleaning rejection rule.

### 4. Clean order items and quarantine rejects

Raw numeric CSV values stay strings until cleaning so malformed values and excess decimal precision are not silently rounded by CSV ingestion. Order-item rows are quarantined for missing/invalid keys or numbers, non-positive quantity, negative price, discount outside `[0, 1]`, or missing parent records. An item belonging to a quarantined order gets `order_quarantined`; a genuinely absent order gets `orphan_order`. The intended line key is `(order_id, product_id)`: identical repeats keep one row and quarantine extras; conflicting payloads for the same key are all quarantined. Orders with unknown customers remain in `quarantine_orders`.

The cleaning code assigns the **first matching reason** in its rule order. A row that violates multiple rules will therefore have one reason recorded, not a complete list of every violation.

### 5. Build the sales fact

`src/joins.py` joins valid order items to cleaned orders, customers, and products. The resulting `fact_sales` grain is one row per valid `(order_id, product_id)` line. The pipeline validates foreign keys and dimension-key uniqueness before joining. Inner joins then keep only valid relationships; unique parent keys prevent fanout, and a row-count guard catches loss or multiplication.

### 6. Calculate amounts and build reports

`src/sales_report.py` calculates line-level amounts using decimal arithmetic:

```text
gross_amount    = quantity × unit_price
discount_amount = gross_amount × discount_pct
net_amount      = gross_amount − discount_amount
```

Gross, discount, and net amounts use `DECIMAL(18,2)`; discounts are rounded to cents per line before net amount is calculated. All six summary datasets filter to status `completed` inside `build_reports`, so callers cannot accidentally include cancelled or returned rows:

1. Total net revenue.
2. Daily net revenue.
3. Net revenue by product category.
4. Net revenue by customer city.
5. Count of distinct completed orders per day.
6. Top five customers by net revenue (customer ID breaks ties).

The detailed `fact_sales` dataset retains valid rows for other statuses, such as `cancelled`; those rows do not enter the six completed-sales reports. Thus the sum of every `fact_sales.net_amount` can differ from the completed-only revenue total.

### 7. Persist and validate

`src/write_outputs.py` writes local Parquet datasets in overwrite mode. `fact_sales` is partitioned by `order_month` (`yyyy-MM`); other outputs are written without an explicit partition column. `src/validate_outputs.py` re-reads Parquet and checks completion markers, required columns, keys, references, value bounds, and revenue reconciliations against the persisted data.

## Outputs

The current pipeline writes these 15 datasets under `output/`:

| Dataset | Contents | Sample rows |
|---|---|---:|
| `cleaned_customers` | Deduplicated customers, normalized city, parsed signup date | 6 |
| `cleaned_products` | Deduplicated products | 5 |
| `cleaned_orders` | Valid, deduplicated orders, including non-completed statuses | 7 |
| `cleaned_order_items` | Valid order lines | 4 |
| `quarantine_customers` | Customers missing the required key | 0 |
| `quarantine_products` | Products missing the required key | 0 |
| `quarantine_orders` | Rejected orders, including one duplicate and one orphan customer | 2 |
| `quarantine_order_items` | Rejected order lines with a reason | 5 |
| `fact_sales` | Joined valid order lines with calculated amounts | 4 |
| `total_revenue` | Completed-only total net revenue | 1 |
| `daily_revenue` | Completed-only revenue by day | 2 |
| `category_revenue` | Completed-only revenue by product category | 3 |
| `city_revenue` | Completed-only revenue by customer city | 2 |
| `completed_orders_per_day` | Distinct completed order count by day | 2 |
| `top_customers` | Up to five completed-sales customers | 2 |

These counts are for the supplied sample fixture, not fixed expectations for other input data. `output/fact_sales/` contains Hive-style month partitions such as `order_month=2024-06/`. All output paths are local filesystem paths relative to the repository.

## Validation and expected sample results

The business checks in `src/validate_outputs.py` currently cover:

- Unique `customer_id` values in cleaned customers.
- Positive quantity and non-negative unit price in sales facts.
- `discount_pct` in `[0, 1]`.
- Completed-only revenue recomputed from the persisted fact table.
- Customer and product references in the sales fact.
- Reconciliation between completed detail net amounts and total revenue.

Then every persisted dataset is re-read from Parquet. Required columns, completion markers, keys, references, numeric bounds, fact grain, completed-only revenue, and daily/category/city reconciliation are checked against the persisted data. The report total is recomputed from persisted completed fact rows.

For the bundled sample data, expected business results include:

- Completed net revenue: **458,000**.
- Daily net revenue: **230,000** on 2024-06-01 and **228,000** on 2024-06-02.
- Category net revenue: Accessories **50,000**, Electronics **180,000**, Furniture **228,000**.
- City net revenue: Shiraz **228,000**, Tehran **230,000**.
- Completed orders per day: one on each of 2024-06-01 and 2024-06-02.
- Top customers: Alice **230,000**, Babak **228,000**.

The raw quality report for the same fixture shows 7 customers, 9 orders, 9 order items, and 6 products; one null customer city; duplicate keys for customer 106, order 5007, and product 205; two invalid customer signup dates; two non-positive quantities; one out-of-range discount; and the orphan references. After cleaning, two orders are quarantined (one duplicate row and one orphan customer); five order-item rows are quarantined (two invalid quantities, one invalid discount, one item for the rejected order, one orphan product).

## Logging and troubleshooting

The entry point configures timestamped Python logging to the console. Spark's own log level is lowered to `ERROR` for the pipeline run. Exceptions are logged with a traceback and re-raised; the Spark session is stopped in a `finally` block. There is no dedicated `logs/` file, rotation policy, or run-specific log file. A future Airflow task can collect the task's standard output and error streams, but an Airflow DAG is not included yet.

Common local messages:

- `hostname ... resolves to a loopback address`: Spark found a usable network interface; this is commonly an environment warning for local execution. If Spark cannot bind or connect, set `SPARK_LOCAL_IP` to the machine's reachable local IP for that run.
- `Unable to load native-hadoop library`: Spark is using its built-in Java implementations; for this local CSV/Parquet exercise this warning has been non-fatal.
- Incubator vector module warning: emitted by the Java runtime/Spark combination; it has not prevented the sample pipeline from running.
- Missing input or permission errors: confirm the four CSV files exist under `data/` and the process can write to `output/` and `reports/`.
- Unexpectedly high counts after a join: inspect dimension-key uniqueness and the row-count guard in `src/joins.py`; duplicate keys in a dimension can multiply fact rows.

## Design notes and current limitations

These are useful context before extending the project:

- **Local-only paths and execution:** Spark uses `local[*]`; inputs, reports, outputs, and `_SUCCESS` checks target the local filesystem. A distributed Airflow worker or cloud cluster needs a shared/object-storage location and a storage-aware success check.
- **Overwrite is not a multi-output transaction:** each dataset is overwritten independently. A failure midway may leave a mix of outputs from different runs. There is no staging directory, atomic publish step, run ID, or rollback.
- **No incremental processing:** each run reads all CSVs and replaces all outputs.
- **Dimension duplicate policy:** duplicate customer and product keys are deduplicated because the supplied repeated rows are identical. Before using production data, decide whether conflicting dimension versions should be quarantined or resolved by an authoritative update timestamp.
- **Quarantine reason precedence:** only the first applicable validation reason is saved. Multi-reason diagnostics would require a reason array or separate rule flags.
- **Repeated Spark actions:** profiling, counts, joins, validations, and writes can evaluate the same lazy lineage multiple times. This is acceptable for the tiny fixture; larger inputs may benefit from selective persistence, fewer actions, and metrics collected in fewer passes.
- **Output validation is rule-focused:** it checks completion markers, required columns, uniqueness, references, amount rules, and revenue reconciliations. It does not enforce a complete versioned schema contract or detect stale mixed-run outputs after a mid-run failure.
- **No CI pipeline:** deterministic business-rule tests are available through Python `unittest`; CI is not configured.
- **No external configuration or secrets management:** paths, app name, local master, and output behavior are fixed in source; there are no credentials or external services in this exercise.
- **Raw report versus run logs:** the Markdown quality report describes raw inputs, while cleaned/quarantine counts are mainly available from pipeline logs and the Parquet datasets.
- **Partitioning demo is instructional:** its daily report contains very few rows. The exercise illustrates shuffle and partition-count behavior; it does not establish an optimal partition count for larger workloads.

## Suggested next steps

A practical order for the next development cycle:

1. Define a business rule for conflicting duplicate customer/product records and document it with the data owner.
2. Add a complete, versioned schema contract for persisted Parquet datasets.
3. Add run-scoped staging and atomic publish so a failed run cannot leave a mixture of output versions.
4. Make input/output paths and Spark settings configurable.
5. If orchestration is required, add an Airflow DAG that captures logs and points workers at shared storage.
6. Add CI to install the pinned dependencies and run the business-rule tests.
7. Revisit caching, partition sizing, and shuffle behavior after measuring representative workloads.

When resuming work, start with `src/main.py` to understand the stage order, then inspect the stage module responsible for the change. Preserve the completed-sales revenue rule and the distinction between the full valid fact and completed-only reports unless the business requirement changes.
