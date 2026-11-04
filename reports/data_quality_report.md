# Data Quality and Reconciliation Report

Raw-input metrics are measured before cleaning. Rejected-row counts and revenue reconciliation are measured after the ETL transformations.

## Row counts

| Table | Rows |
|---|---:|
| `customers` | 7 |
| `orders` | 9 |
| `order_items` | 9 |
| `products` | 6 |

## Null counts

| Table | Column | Null rows |
|---|---|---:|
| `customers` | `customer_id` | 0 |
| `customers` | `full_name` | 0 |
| `customers` | `city` | 1 |
| `customers` | `signup_date` | 0 |
| `customers` | `segment` | 0 |
| `orders` | `order_id` | 0 |
| `orders` | `customer_id` | 0 |
| `orders` | `order_date` | 0 |
| `orders` | `status` | 0 |
| `orders` | `payment_method` | 0 |
| `order_items` | `order_id` | 0 |
| `order_items` | `product_id` | 0 |
| `order_items` | `quantity` | 0 |
| `order_items` | `unit_price` | 0 |
| `order_items` | `discount_pct` | 0 |
| `products` | `product_id` | 0 |
| `products` | `product_name` | 0 |
| `products` | `category` | 0 |

## Duplicate keys

| Table | Key column | Key value | Rows with key | Extra rows |
|---|---|---:|---:|---:|
| `customers` | `customer_id` | `106` | 2 | 1 |
| `orders` | `order_id` | `5007` | 2 | 1 |
| `products` | `product_id` | `205` | 2 | 1 |

## Duplicate order-item keys

The intended order-line key is `(order_id, product_id)`. `payload_versions` shows whether repeated keys carry identical or conflicting quantity/price/discount values.

| order_id | product_id | Rows | Extra rows | Payload versions |
|---:|---:|---:|---:|---:|
| — | — | 0 | 0 | 0 |

## Invalid dates

| Table and column | Invalid rows |
|---|---:|
| `customers.signup_date` | 2 |
| `orders.order_date` | 0 |

## Malformed, over-precision, and out-of-range numeric values

| Check | Rows |
|---|---:|
| `malformed_quantity` | 0 |
| `malformed_unit_price` | 0 |
| `malformed_discount_pct` | 0 |
| `precision_unit_price` | 0 |
| `precision_discount_pct` | 0 |
| `out_of_range_unit_price` | 0 |
| `out_of_range_discount_pct` | 0 |

## Order-item rule violations

| Rule | Rows |
|---|---:|
| quantity <= 0 | 2 |
| unit_price < 0 | 0 |
| discount_pct outside [0, 1] | 1 |

## Order-status quality

| Status check | Rows |
|---|---:|
| `missing_status` | 0 |
| `invalid_status` | 0 |

## Orphan foreign keys

| Relationship | Rows without a parent match |
|---|---:|
| `orders.customer_id -> customers.customer_id` | 1 |
| `order_items.order_id -> orders.order_id` | 0 |
| `order_items.product_id -> products.product_id` | 1 |

## Rejected rows after cleaning

Each row is assigned its first matching rejection reason. A row with multiple defects appears once under the highest-priority reason selected by the cleaning rules.

| Quarantine dataset | Reject reason | Rows |
|---|---|---:|
| `quarantine_customers` | — | 0 |
| `quarantine_products` | — | 0 |
| `quarantine_orders` | `duplicate_order` | 1 |
| `quarantine_orders` | `orphan_customer` | 1 |
| `quarantine_order_items` | `invalid_discount_pct` | 1 |
| `quarantine_order_items` | `invalid_quantity` | 2 |
| `quarantine_order_items` | `order_quarantined` | 1 |
| `quarantine_order_items` | `orphan_product` | 1 |

## Revenue reconciliation

| Measure | Net amount | Explanation |
|---|---:|---|
| All valid fact rows | 558000.00 | Includes valid non-completed orders |
| Completed orders | 458000.00 | Revenue used by the business reports |
| Excluded status difference | 100000.00 | Non-completed valid fact rows excluded from reports |
