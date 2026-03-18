"""
KICKZ EMPIRE — Gold Layer (Business Aggregations)
==================================================
TP1 — Step 3: Create Gold tables/views from Silver.

The Gold layer contains aggregated tables and views, ready for
analytics dashboards and business reports.

Gold tables/views created:
    1. gold.daily_revenue       — Revenue per day
    2. gold.product_performance — Product performance (sales, revenue, qty)
    3. gold.customer_ltv        — Lifetime Value per customer
"""

import pandas as pd
from sqlalchemy import text

from src.database import get_engine, SILVER_SCHEMA, GOLD_SCHEMA


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _read_silver(table_name: str) -> pd.DataFrame:
    """Read a table from the Silver schema."""
    engine = get_engine()
    query = f"SELECT * FROM {SILVER_SCHEMA}.{table_name}"
    return pd.read_sql(query, engine)


def _create_gold_table(df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
    """Load a DataFrame into a Gold schema table."""
    engine = get_engine()
    df.to_sql(
        name=table_name,
        con=engine,
        schema=GOLD_SCHEMA,
        if_exists=if_exists,
        index=False,
    )
    print(f"    ✅ {GOLD_SCHEMA}.{table_name} — {len(df)} rows")


def _create_gold_view(view_name: str, sql: str):
    """
    Create a SQL view in the Gold schema.

    Args:
        view_name (str): View name (without the schema).
        sql (str): The SELECT query that defines the view.
    """
    engine = get_engine()
    full_name = f"{GOLD_SCHEMA}.{view_name}"
    with engine.connect() as conn:
        conn.execute(text(f"DROP VIEW IF EXISTS {full_name}"))
        conn.execute(text(f"CREATE VIEW {full_name} AS {sql}"))
        conn.commit()
    print(f"    ✅ View {full_name} created")


# ---------------------------------------------------------------------------
# Gold tables / views
# ---------------------------------------------------------------------------
def create_daily_revenue():
    """
    Create gold.daily_revenue — Revenue per day.

    Expected columns:
        - order_date (DATE)      : Day
        - total_orders (INT)     : Number of orders
        - total_revenue (FLOAT)  : Sum of total_usd
        - avg_order_value (FLOAT): Average order value
        - total_items (INT)      : Total number of items sold

    Source: silver.fct_orders (join with silver.fct_order_lines)

    SQL Hint:
        SELECT
            DATE(o.order_date) AS order_date,
            COUNT(DISTINCT o.order_id) AS total_orders,
            SUM(o.total_usd) AS total_revenue,
            AVG(o.total_usd) AS avg_order_value,
            SUM(ol.quantity) AS total_items
        FROM {SILVER_SCHEMA}.fct_orders o
        LEFT JOIN {SILVER_SCHEMA}.fct_order_lines ol ON o.order_id = ol.order_id
        WHERE o.status NOT IN ('cancelled', 'chargeback')
        GROUP BY DATE(o.order_date)
        ORDER BY order_date
    """
    print("  📊 Gold: daily_revenue")

    # TODO: Create daily_revenue using SQL
    # Write a SQL query that joins fct_orders + fct_order_lines,
    # groups by date, and computes the aggregates described in the docstring.
    # Exclude cancelled/chargeback orders.
    # Then: pd.read_sql() → _create_gold_table()
    sql = f"""
        SELECT
            DATE(o.order_date) AS order_date,
            COUNT(DISTINCT o.order_id) AS total_orders,
            SUM(o.total_usd) AS total_revenue,
            AVG(o.total_usd) AS avg_order_value,
            SUM(ol.quantity) AS total_items
        FROM {SILVER_SCHEMA}.fct_orders o
        LEFT JOIN {SILVER_SCHEMA}.fct_order_lines ol ON o.order_id = ol.order_id
        WHERE o.status NOT IN ('cancelled', 'chargeback')
        GROUP BY DATE(o.order_date)
        ORDER BY order_date
    """

    engine = get_engine()
    df = pd.read_sql(sql, engine)
    _create_gold_table(df, "daily_revenue")
    


def create_product_performance():
    """
    Create gold.product_performance — Metrics per product.

    Expected columns:
        - product_id (STRING)     : Product ID
        - product_name (STRING)   : Product name
        - brand (STRING)          : Brand
        - category (STRING)       : Category
        - total_quantity_sold (INT) : Total quantity sold
        - total_revenue (FLOAT)   : Total revenue
        - num_orders (INT)        : Number of orders
        - avg_unit_price (FLOAT)  : Average selling price

    Source: silver.fct_order_lines JOIN silver.dim_products

    Hint:
        Group by product_id, product_name, brand, category
        Aggregate quantity, line_total_usd, count distinct order_id
    """
    print("  🏆 Gold: product_performance")

    # TODO: Create the product_performance table
    # Join fct_order_lines with dim_products (and filter via fct_orders)
    # Group by product_id + product details, aggregate sales metrics
    # See the expected columns in the docstring above

    sql = f"""
        SELECT
            p.product_id,
            p.model_name,
            p.brand,
            p.category,
            SUM(ol.quantity) AS total_quantity_sold,
            SUM(ol.line_total_usd) AS total_revenue,
            COUNT(DISTINCT ol.order_id) AS num_orders,
            AVG(ol.unit_price_usd) AS avg_unit_price
        FROM {SILVER_SCHEMA}.fct_order_lines ol
        JOIN {SILVER_SCHEMA}.dim_products p ON ol.product_id = p.product_id
        JOIN {SILVER_SCHEMA}.fct_orders o ON ol.order_id = o.order_id
        WHERE o.status NOT IN ('cancelled', 'chargeback')
        GROUP BY p.product_id, p.model_name, p.brand, p.category
        ORDER BY total_revenue DESC
    """

    engine = get_engine()
    df = pd.read_sql(sql, engine)
    _create_gold_table(df, "product_performance")


def create_customer_ltv():
    """
    Create gold.customer_ltv — Lifetime Value per customer.

    Expected columns:
        - user_id (INT)           : Customer ID
        - email (STRING)          : Email
        - first_name (STRING)     : First name
        - last_name (STRING)      : Last name
        - loyalty_tier (STRING)   : Loyalty tier
        - total_orders (INT)      : Total number of orders
        - total_spent (FLOAT)     : Total amount spent
        - avg_order_value (FLOAT) : Average order value
        - first_order_date (DATE) : First order date
        - last_order_date (DATE)  : Last order date
        - days_as_customer (INT)  : Customer tenure in days

    Source: silver.fct_orders JOIN silver.dim_users

    Hint:
        Group by user_id, join with dim_users for customer info
        - first_order_date = MIN(order_date)
        - last_order_date = MAX(order_date)
        - days_as_customer = last_order_date - first_order_date
    """
    print("  💰 Gold: customer_ltv")

    # TODO: Create the customer_ltv table
    # Join fct_orders with dim_users
    # Group by customer, compute the aggregates listed in the docstring
    # Hint: MIN/MAX for dates, EXTRACT(DAY FROM ...) for tenure
    sql = f"""
        SELECT
            u.user_id,
            u.email,
            u.first_name,
            u.last_name,
            u.loyalty_tier,
            COUNT(DISTINCT o.order_id) AS total_orders,
            SUM(o.total_usd) AS total_spent,
            AVG(o.total_usd) AS avg_order_value,
            MIN(o.order_date) AS first_order_date,
            MAX(o.order_date) AS last_order_date,
            EXTRACT(DAY FROM MAX(o.order_date) - MIN(o.order_date)) AS days_as_customer
        FROM {SILVER_SCHEMA}.fct_orders o
        JOIN {SILVER_SCHEMA}.dim_users u ON o.user_id = u.user_id
        WHERE o.status NOT IN ('cancelled', 'chargeback')
        GROUP BY u.user_id, u.email, u.first_name, u.last_name, u.loyalty_tier
        ORDER BY total_spent DESC
    """

    engine = get_engine()
    df = pd.read_sql(sql, engine)
    _create_gold_table(df, "customer_ltv") 
    


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------
def create_gold_layer():
    """
    Create all Gold layer tables/views.
    """
    print(f"\n{'='*60}")
    print(f"  🥇 GOLD Layer ({GOLD_SCHEMA})")
    print(f"{'='*60}\n")


    create_daily_revenue()
    create_product_performance()
    create_customer_ltv()

    print(f"\n  ✅ Gold layer created in {GOLD_SCHEMA}")
    

    print(f"\n  ✅ Gold layer created in {GOLD_SCHEMA}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "_main_":
    create_gold_layer()