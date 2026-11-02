import csv
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# NULL is written as the literal text "NULL", like in the PDF.
# In Spark: spark.read.option("nullValue", "NULL")

FILES = {
    "customers.csv": (
        ["customer_id", "full_name", "city", "signup_date", "segment"],
        [
            (101, "Alice", "Tehran", "2024-01-10", "Gold"),
            (102, "Babak", "Shiraz", "2024-02-15", "Silver"),
            (103, "Leila", "Tehran", "2024-03-03", "Gold"),
            (104, "Reza", "NULL", "2024-04-20", "Bronze"),         # NULL city
            (105, "Neda", "Tabriz", "2024-05-11", "Silver"),
            (106, "Sam", "Isfahan", "invalid-date", "Gold"),       # bad date
            (106, "Sam", "Isfahan", "invalid-date", "Gold"),       # duplicate
        ],
    ),
    "orders.csv": (
        ["order_id", "customer_id", "order_date", "status", "payment_method"],
        [
            (5001, 101, "2024-06-01", "completed", "card"),
            (5002, 102, "2024-06-02", "completed", "cash"),
            (5003, 103, "2024-06-03", "cancelled", "card"),
            (5004, 104, "2024-06-04", "completed", "online"),
            (5005, 999, "2024-06-05", "completed", "card"),        # orphan customer
            (5006, 101, "2024-06-07", "returned", "card"),
            (5007, 105, "2024-06-08", "completed", "card"),
            (5007, 105, "2024-06-08", "completed", "card"),        # duplicate
            (5008, 106, "2024-06-09", "completed", "card"),
        ],
    ),
    "order_items.csv": (
        ["order_id", "product_id", "quantity", "unit_price", "discount_pct"],
        [
            (5001, 201, 2, 100000, "0.10"),
            (5001, 202, 1, 50000, "0.00"),
            (5002, 203, 3, 80000, "0.05"),
            (5003, 201, 1, 100000, "0.00"),
            (5004, 204, 0, 120000, "0.10"),                        # quantity 0
            (5005, 201, 2, 100000, "0.00"),
            (5006, 202, -1, 50000, "0.00"),                        # quantity -1
            (5007, 205, 2, 90000, "1.50"),                         # discount > 1
            (5008, 999, 1, 50000, "0.00"),                         # orphan product
        ],
    ),
    "products.csv": (
        ["product_id", "product_name", "category"],
        [
            (201, "Laptop", "Electronics"),
            (202, "Mouse", "Accessories"),
            (203, "Chair", "Furniture"),
            (204, "Desk", "Furniture"),
            (205, "Keyboard", "Accessories"),
            (205, "Keyboard", "Accessories"),                      # duplicate
        ],
    ),
}

for name, (header, rows) in FILES.items():
    path = DATA_DIR / name
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"{name}: {len(rows)} rows -> {path}")