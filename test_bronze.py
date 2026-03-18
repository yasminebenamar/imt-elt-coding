
# 🔹 Bronze Layer Validation Script
import os
import pandas as pd
from src.database import get_engine

# --- Load RDS credentials from environment variables ---
RDS_HOST = os.getenv("RDS_HOST")
RDS_PORT = int(os.getenv("RDS_PORT", "5432"))
RDS_DATABASE = os.getenv("RDS_DATABASE")
RDS_USER = os.getenv("RDS_USER")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")

BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "bronze_group6")

# --- Create SQLAlchemy engine ---
engine = get_engine()

# --- Helper function to run query and print ---
def run_query(query: str, description: str):
    print(f"\n {description}")
    df = pd.read_sql(query, engine)
    print(df)
    return df

# List all tables in bronze schema
tables_query = f"""
SELECT table_name
FROM information_schema.tables
WHERE table_schema = '{BRONZE_SCHEMA}'
ORDER BY table_name;
"""
tables_df = run_query(tables_query, "Tables in Bronze Schema")

# Check table count
num_tables = len(tables_df)
print(f"\n✅ Number of tables: {num_tables}")
if num_tables == 6:
    print("✅ Expected 6 tables in Bronze schema!")
else:
    print("⚠️ Unexpected number of tables.")

# Row counts per table
counts_query = f"""
SELECT 'products' AS table_name, COUNT(*) AS rows FROM {BRONZE_SCHEMA}.products
UNION ALL
SELECT 'users', COUNT(*) FROM {BRONZE_SCHEMA}.users
UNION ALL
SELECT 'orders', COUNT(*) FROM {BRONZE_SCHEMA}.orders
UNION ALL
SELECT 'order_line_items', COUNT(*) FROM {BRONZE_SCHEMA}.order_line_items
UNION ALL
SELECT 'reviews', COUNT(*) FROM {BRONZE_SCHEMA}.reviews
UNION ALL
SELECT 'clickstream', COUNT(*) FROM {BRONZE_SCHEMA}.clickstream;
"""
counts_df = run_query(counts_query, "Row counts per table")

# Inspect columns of products table
columns_query = f"""
SELECT column_name
FROM information_schema.columns
WHERE table_schema = '{BRONZE_SCHEMA}' AND table_name = 'products'
ORDER BY ordinal_position;
"""
columns_df = run_query(columns_query, "Columns of products table")
print(f"\n✅ Products table has {len(columns_df)} columns (expected ~21)")

#  Order statuses
status_query = f"""
SELECT status, COUNT(*) AS cnt
FROM {BRONZE_SCHEMA}.orders
GROUP BY status
ORDER BY cnt DESC;
"""
status_df = run_query(status_query, "Order statuses")
print("\n✅ Expected statuses: delivered, shipped, processing, returned, cancelled, chargeback")

#  Review ratings
ratings_query = f"""
SELECT rating, COUNT(*) AS cnt
FROM {BRONZE_SCHEMA}.reviews
GROUP BY rating
ORDER BY rating;
"""
ratings_df = run_query(ratings_query, "Review ratings")
print("\n✅ Expected ratings: 1–5 (integer)")

# Clickstream event types
events_query = f"""
SELECT event_type, COUNT(*) AS cnt
FROM {BRONZE_SCHEMA}.clickstream
GROUP BY event_type
ORDER BY cnt DESC;
"""
events_df = run_query(events_query, "Clickstream event types")
print("\n✅ Expected event types: page_view, product_view, add_to_cart, checkout, etc.")

# --- Final Checkpoint ---
if num_tables == 6 and all(counts_df['rows'] > 0):
    print("\n Final Checkpoint: 6 populated tables in Bronze schema")
else:
    print("\n⚠️ Checkpoint incomplete: verify table counts and data.")