# Raw Data Quality Report

This report describes the input data before cleaning.

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

## Invalid dates

| Table and column | Invalid rows |
|---|---:|
| `customers.signup_date` | 2 |
| `orders.order_date` | 0 |

## Order item rule violations

| Rule | Rows |
|---|---:|
| quantity <= 0 | 2 |
| unit_price < 0 | 0 |
| discount_pct outside [0, 1] | 1 |

## Orphan foreign keys

| Relationship | Rows without a parent match |
|---|---:|
| `orders.customer_id -> customers.customer_id` | 1 |
| `order_items.order_id -> orders.order_id` | 0 |
| `order_items.product_id -> products.product_id` | 1 |
