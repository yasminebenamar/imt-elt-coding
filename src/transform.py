"""
KICKZ EMPIRE — Transform (Silver Layer)
=========================================
TP1 — Step 2: Clean and conform Bronze data → Silver.

This module reads tables from the Bronze schema, applies cleaning
transformations, and loads the results into the Silver schema.

Transformations applied:
    - Remove internal columns (prefixed with _)
    - Normalize data types
    - Remove PII (Personally Identifiable Information)
    - Validate values (statuses, amounts, etc.)

Silver tables created:
    1. silver.dim_products   ← bronze.products (cleaned)
    2. silver.dim_users      ← bronze.users (PII removed)
    3. silver.fct_orders     ← bronze.orders (conformed)
    4. silver.fct_order_lines ← bronze.order_line_items (conformed)
"""

import pandas as pd
from sqlalchemy import text

from src.database import get_engine, BRONZE_SCHEMA, SILVER_SCHEMA


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _read_bronze(table_name: str) -> pd.DataFrame:
    """
    Read a table from the Bronze schema via SQL.

    Args:
        table_name (str): Bronze table name (e.g. "products").

    Returns:
        pd.DataFrame: The Bronze table contents.

    Hint: use pd.read_sql() with a SELECT * query
    Docs: https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html
    """
    engine = get_engine()
    query = f"SELECT * FROM {BRONZE_SCHEMA}.{table_name}"
    return pd.read_sql(query, engine)


def _drop_internal_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop all columns whose name starts with '_'.
    These columns are internal data that should not be exposed.

    Args:
        df (pd.DataFrame): The source DataFrame.

    Returns:
        pd.DataFrame: The DataFrame without internal columns.

    Example:
        Columns before: ['product_id', 'brand', '_internal_cost_usd', '_supplier_id']
        Columns after:  ['product_id', 'brand']
    """
    # Filter and drop columns starting with '_'
    # Steps: 1. Find columns that start with '_' (list comprehension on df.columns)
    #        2. Drop them with df.drop(columns=...)
    #        3. Print how many were removed, then return
    cols_to_drop = [col for col in df.columns if col.startswith("_")]
    df = df.drop(columns=cols_to_drop)
    print(f"  Dropped {len(cols_to_drop)} internal columns: {cols_to_drop}")
    return df


def _load_to_silver(df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
    """
    Load a DataFrame into a Silver schema table.

    Args:
        df (pd.DataFrame): The cleaned data.
        table_name (str): Target table name (without the schema).
        if_exists (str): "replace" or "append"
    """
    engine = get_engine()
    df.to_sql(
        name=table_name,
        con=engine,
        schema=SILVER_SCHEMA,
        if_exists=if_exists,
        index=False,)
    print(f" ✅ {SILVER_SCHEMA}.{table_name} — {len(df)} rows loaded")


# ---------------------------------------------------------------------------
# Transformations per table
# ---------------------------------------------------------------------------
def transform_products() -> pd.DataFrame:
    """
    Transform bronze.products → silver.dim_products.

    Transformations:
        1. Drop internal columns (_internal_cost_usd, _supplier_id, etc.)
        2. Parse available_sizes_json: it's a JSON string inside the CSV
           → keep as-is for now (or validate)
        3. Normalize tags: replace the '|' separator with ','
        4. Ensure price_usd is a valid float (no negatives)
        5. Convert is_active and is_hype_product to booleans

    Returns:
        pd.DataFrame: The cleaned catalog.
    """
    print("  📦 Transform: products → dim_products")
    
    df = _read_bronze("products")

    # Step 1 — Drop internal columns
    df = _drop_internal_columns(df)

    # Step 2 — Normalize tags (replace '|' with ', ')
    df["tags"] = df["tags"].str.replace("|", ", ", regex=False)

    # Step 3 — Validate price_usd (remove rows where price <= 0)
    df = df[df["price_usd"] > 0]

    # Step 4 — Convert boolean columns
    df["is_active"] = df["is_active"].astype(bool)
    df["is_hype_product"] = df["is_hype_product"].astype(bool)

    # Step 5 — Load into Silver
    _load_to_silver(df, "dim_products")

    return df


def transform_users() -> pd.DataFrame:
    """
    Transform bronze.users → silver.dim_users.

    Transformations:
        1. Drop internal columns (_hashed_password, _ga_client_id,
           _fbp, _device_fingerprint, _last_ip, _failed_login_count,
           _account_flags, _internal_segment_id)
        2. Replace NULL loyalty_tier with 'none' (unclassified)
        3. Normalize emails to lowercase
        4. Remove/mask unnecessary PII (phone → keep only the country)

    ⚠️  Warning about sensitive data: NEVER expose passwords,
        IPs, or fingerprints in the Silver layer.

    Returns:
        pd.DataFrame: The cleaned users (without sensitive PII).
    """
    print("  👤 Transform: users → dim_users")
    df = _read_bronze("users")

    # Step 1 — Drop internal columns (especially PII: passwords, IPs, fingerprints)
    df= _drop_internal_columns(df)
    # Step 2 — Replace NULL loyalty_tier with 'none'
    # Look at: .fillna()
    df["loyalty_tier"] = df["loyalty_tier"].fillna("none")

    # Step 3 — Normalize emails (lowercase + strip whitespace)
    df["email"]= df["email"].str.lower().str.strip()

    # Step 4 — Mask phone (keep only country code)
    df["phone"] = df["phone"].str.extract(r"(^\+\d+)")

    # Step 5 — Load into Silver as "dim_users"
    _load_to_silver(df,"dim_users")

    return df


def transform_orders() -> pd.DataFrame:
    """
    Transform bronze.orders → silver.fct_orders.

    Transformations:
        1. Drop internal columns (_stripe_*, _paypal_*, _fraud_score, etc.)
        2. Validate the `status` field (must be in the allowed list)
           Valid statuses: delivered, shipped, processing, returned, cancelled, chargeback
        3. Convert `order_date` to datetime
        4. Verify that total_usd = subtotal_usd - discount_amount_usd + shipping_cost_usd + tax_usd
           (tolerance of 0.01 for rounding)
        5. Replace NULL coupon_code with '' (empty string)

    Returns:
        pd.DataFrame: The cleaned orders.
    """
    print("  🛍️ Transform: orders → fct_orders")
    df = _read_bronze("orders")

    # Step 1 — Drop internal columns
    df = _drop_internal_columns(df)
    #  Step 2 — Validate statuses
    # Only keep rows with a valid status. The valid set is in the docstring above.
    # Look at: .isin() and boolean indexing
    valid_status =["delivered", "shipped", "processing", "returned", "cancelled", "chargeback"]
    df = df[df["status"].isin(valid_status)]
    # Step 3 — Convert order_date to a proper datetime type
    # Look at: pd.to_datetime()
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df = df[df["order_date"].notna()]
    # Step 4 — Replace NULL coupon_code with empty string
    # Look at: .fillna()
    if all(col in df.columns for col in ["total_usd", "subtotal_usd", "discount_amount_usd","shipping_cost_usd", "tax_usd"]):
        expected_total = (df["subtotal_usd"]
            - df["discount_amount_usd"]
            + df["shipping_cost_usd"]
            + df["tax_usd"])
        diff = (df["total_usd"] - expected_total).abs()

        invalid = diff > 0.01
        if invalid.any():
            print(f"{invalid.sum()} inconsistent totals detected")

    # Step 5 — Replace NULL coupon_code
    if "coupon_code" in df.columns:
        df["coupon_code"] = df["coupon_code"].fillna("")

    # Step 6 — Load into Silver
    _load_to_silver(df, "fct_orders")

    return df


def transform_order_line_items() -> pd.DataFrame:
    """
    Transform bronze.order_line_items → silver.fct_order_lines.

    Transformations:
        1. Drop internal columns (_warehouse_id, _internal_batch_code, _pick_slot)
        2. Verify that line_total_usd ≈ unit_price_usd * quantity
        3. Ensure quantity > 0
        4. Check referential integrity: all order_id values must exist in fct_orders

    Returns:
        pd.DataFrame: The cleaned order line items.
    """
    print("  📋 Transform: order_line_items → fct_order_lines")
    df = _read_bronze("order_line_items")

    # Step 1 — Drop internal columns
    df = _drop_internal_columns(df)
    # Step 2 — Validate quantity > 0 (remove invalid rows)
    df = df[df['quantity'] > 0]
    # Step 3 — Verify line_total_usd ≈ unit_price_usd * quantity
    # Compute the difference, flag rows where abs(diff) > 0.01, then clean up
    # This is a data quality check — print how many bad rows you find
    total_expected = df['unit_price_usd'] * df['quantity']
    difference = df['line_total_usd'] - total_expected
    invalid_rows = df[abs(difference) > 0.01]
    print(f"  {len(invalid_rows)} bad rows with line")
    df = df[abs(difference) <= 0.01]
    # Step 4 — Load into Silver as "fct_order_lines"
    _load_to_silver(df, "fct_order_lines")

    return df

# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------
def transform_all() -> dict[str, pd.DataFrame]:
    """
    Run the full Bronze → Silver transformation.

    Returns:
        dict: {table_name: DataFrame} for each transformed table.
    """
    print(f"\n{'='*60}")
    print(f"  🥈 TRANSFORM → Silver ({SILVER_SCHEMA})")
    print(f"{'='*60}\n")

    results = {}

    # Call each transform_*() function and store the result in the dict
    # There are 4 functions to call, each returns a DataFrame
    # Keys should match the Silver table names: dim_products, dim_users, fct_orders, fct_order_lines
    results["dim_products"] = transform_products()
    results["dim_users"] = transform_users()
    results["fct_orders"] = transform_orders()
    results["fct_order_lines"] = transform_order_line_items()


    print(f"\n  ✅ Transformation complete — {len(results)} tables in {SILVER_SCHEMA}")
    return results


# ---------------------------------------------------------------------------
# Entry point for testing the transformation standalone
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    transform_all()
