"""
KICKZ EMPIRE — Extract (Bronze Layer)
======================================
TP1 — Step 2: Load raw data from S3 → Bronze schema in PostgreSQL.

This module reads files from the S3 data lake (bucket: kickz-empire-data)
and loads them as-is into your group's Bronze schema.

The data lake contains **3 different file formats**:
    - CSV   : simple tabular files
    - JSONL : one JSON object per line (newline-delimited JSON)
    - Parquet (partitioned) : columnar format, split by date (dt=YYYY-MM-DD/)

Datasets handled in this lab:
    CSV:
        1. raw/catalog/products.csv                         → bronze.products
        2. raw/users/users.csv                              → bronze.users
        3. raw/orders/orders.csv                            → bronze.orders
        4. raw/order_line_items/order_line_items.csv        → bronze.order_line_items
    JSONL:
        5. raw/reviews/reviews.jsonl                        → bronze.reviews
    Parquet (partitioned by day):
        6. raw/clickstream/dt=YYYY-MM-DD/part-*.snappy.parquet → bronze.clickstream

Bronze principle: data is loaded AS-IS, with zero transformations.
We even keep the "dirty" columns (_internal_*, _hashed_password, etc.)
"""

import os
from io import StringIO, BytesIO

import boto3
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import text

from src.database import get_engine, BRONZE_SCHEMA


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "kickz-empire-data")
S3_PREFIX = os.getenv("S3_PREFIX", "raw")   # root prefix in the bucket
AWS_REGION = os.getenv("AWS_REGION", "eu-west-3")


# ---------------------------------------------------------------------------
# Helpers — S3 client
# ---------------------------------------------------------------------------
def _get_s3_client():
    """
    Create and return a boto3 S3 client.

    Uses AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from environment
    variables (loaded via .env).

    Returns:
        boto3.client: An S3 client configured for the project region.
    """
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


# ---------------------------------------------------------------------------
# Helpers — Read different formats from S3
# ---------------------------------------------------------------------------
def _read_csv_from_s3(s3_key: str) -> pd.DataFrame:
    """
    Read a CSV file from S3 into a pandas DataFrame.

    Args:
        s3_key (str): Full S3 object key (e.g. "raw/catalog/products.csv")

    Returns:
        pd.DataFrame: The CSV contents.

    Hint: use boto3 to get the object, then pd.read_csv() on the body.
    Docs:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
    """
    # TODO: Download the CSV from S3 and return it as a DataFrame
    # Steps: get S3 client → get_object() → read & decode the body → pd.read_csv()
    # Remember: read_csv() expects a file-like object, not a raw string
    s3=boto3.client("s3")
    response = s3.get_object(Bucket="kickz-empire-data", Key=s3_key)
    df = pd.read_csv(StringIO(response["Body"].read().decode("utf-8")))
    return df

def _read_jsonl_from_s3(s3_key: str) -> pd.DataFrame:
    """
    Read a JSONL (newline-delimited JSON) file from S3 into a DataFrame.

    JSONL format = one JSON object per line. Each line is a complete record.
    Example line: {"review_id": "abc", "rating": 5, "body": "Great!"}

    Args:
        s3_key (str): Full S3 object key (e.g. "raw/reviews/reviews.jsonl")

    Returns:
        pd.DataFrame: The JSONL contents.

    Hint: use boto3 to get the object, then pd.read_json() with lines=True.
    Docs:
        https://pandas.pydata.org/docs/reference/api/pandas.read_json.html
    """
    # TODO: Download the JSONL from S3 and return it as a DataFrame
    # Very similar to _read_csv_from_s3(), but use pd.read_json() instead.
    # Key parameter: lines=True (tells pandas each line is a separate JSON object)
    s3=boto3.client("s3")
    response = s3.get_object(Bucket="kickz-empire-data", Key=s3_key)
    jsonl_content = response["Body"].read().decode("utf-8")
    df = pd.read_json(StringIO(jsonl_content), lines=True)
    return(df)


def _read_partitioned_parquet_from_s3(s3_prefix: str) -> pd.DataFrame:
    """
    Read a date-partitioned Parquet dataset from S3 into a DataFrame.

    The clickstream data is stored as partitioned Parquet:
        raw/clickstream/dt=2026-02-05/part-00001.snappy.parquet
        raw/clickstream/dt=2026-02-05/part-00002.snappy.parquet
        raw/clickstream/dt=2026-02-06/part-00001.snappy.parquet
        ...

    Strategy:
        1. List all objects under the given S3 prefix
        2. Filter for .parquet files only
        3. Download each file and read it with pyarrow
        4. Concatenate all partitions into a single DataFrame

    Args:
        s3_prefix (str): S3 prefix for the partitioned dataset
                         (e.g. "raw/clickstream/")

    Returns:
        pd.DataFrame: All partitions concatenated.

    Docs:
        https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html
    """
    # TODO: List all Parquet files under s3_prefix and concatenate them
    s3=boto3.client("s3")
    bucket ="kickz-empire-data"
    paginator=s3.get_paginator("list_objects_v2")
    dataframes = []

    #   1. Use s3.get_paginator("list_objects_v2") to list all objects under the prefix
    for page in paginator.paginate(Bucket=bucket, Prefix=s3_prefix):
        if "Contents" not in page:
            continue

        #   2. Filter keys that end with ".parquet"
        for obj in page["Contents"]:
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue

            #   3. For each file: download with get_object(), read with pq.read_table()
            #   (Parquet is binary → use BytesIO, not StringIO)
            response = s3.get_object(Bucket=bucket, Key=key)
            buffer = BytesIO(response["Body"].read())

            table = pq.read_table(buffer)
            df = table.to_pandas()

            dataframes.append(df)

    #   4. Collect all DataFrames in a list, then pd.concat() them
    if not dataframes:
        return pd.DataFrame()

    return pd.concat(dataframes, ignore_index=True)

# ---------------------------------------------------------------------------
# Helper — Load to Bronze
# ---------------------------------------------------------------------------
def _load_to_bronze(df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
    """
    Load a DataFrame into a Bronze schema table.

    Args:
        df (pd.DataFrame): The data to load.
        table_name (str): Target table name (without the schema).
        if_exists (str): Behavior if the table already exists.
            - "replace": drop and recreate the table
            - "append" : add data to the existing table

    Hint: use df.to_sql() with these parameters:
        - name: table name
        - con: SQLAlchemy engine
        - schema: the bronze schema
        - if_exists: "replace" or "append"
        - index: False (don't write the pandas index)

    Docs:
        https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
    """
    # TODO: Load the DataFrame into PostgreSQL using df.to_sql()
    # You'll need: get_engine(), and the right to_sql() parameters
    # Don't forget: index=False (we don't want the pandas index as a column)
    engine=get_engine()
    df.to_sql(name=table_name, con=engine,schema=BRONZE_SCHEMA, if_exists=if_exists, index=False)


# ---------------------------------------------------------------------------
# Extract functions — CSV datasets
# ---------------------------------------------------------------------------
def extract_products() -> pd.DataFrame:
    """
    Extract the product catalog from S3 and load it into bronze.products.

    Format: CSV
    S3 key: raw/catalog/products.csv

    Steps:
        1. Read products.csv from S3 using _read_csv_from_s3()
        2. Print the number of rows and columns
        3. Load into bronze.products using _load_to_bronze()

    Returns:
        pd.DataFrame: The catalog data.
    """
    # TODO: Read → Log → Load → Return
    # Use _read_csv_from_s3() with the right S3 key, then _load_to_bronze()
    df=_read_csv_from_s3("raw/catalog/products.csv")
    print(f"✅ bronze_group6.products loaded {df.shape[0]} rows, {df.shape[1]} columns")
    _load_to_bronze(df, table_name="products", if_exists="replace")
    return df


def extract_users() -> pd.DataFrame:
    """
    Extract users from S3 and load them into bronze.users.

    Format: CSV
    S3 key: raw/users/users.csv

    Returns:
        pd.DataFrame: The user data.
    """
    df=_read_csv_from_s3("raw/users/users.csv")
    print(f"✅ bronze_group6.orders loaded {df.shape[0]} rows, {df.shape[1]} columns")
    _load_to_bronze(df, table_name="users", if_exists="replace")
    return df


def extract_orders() -> pd.DataFrame:
    """
    Extract orders from S3 and load them into bronze.orders.

    Format: CSV
    S3 key: raw/orders/orders.csv

    Returns:
        pd.DataFrame: The order data.
    """
    df=_read_csv_from_s3("raw/orders/orders.csv")
    print(f"✅ bronze_group6.orders loaded {df.shape[0]} rows, {df.shape[1]} columns")
    _load_to_bronze(df, table_name="orders", if_exists="replace")
    return df


def extract_order_line_items() -> pd.DataFrame:
    """
    Extract order line items from S3 and load them into bronze.order_line_items.

    Format: CSV
    S3 key: raw/order_line_items/order_line_items.csv

    Returns:
        pd.DataFrame: The order line item data.
    """
    # TODO: Same pattern as extract_products()
    df=_read_csv_from_s3("raw/order_line_items/order_line_items.csv")
    print(f"✅ bronze_group6.order_line_items loaded {df.shape[0]} rows, {df.shape[1]} columns")
    _load_to_bronze(df, table_name="order_line_items", if_exists="replace")
    return df


# ---------------------------------------------------------------------------
# Extract functions — JSONL datasets
# ---------------------------------------------------------------------------
def extract_reviews() -> pd.DataFrame:
    """
    Extract customer reviews from S3 and load them into bronze.reviews.

    Format: JSONL (newline-delimited JSON)
    S3 key: raw/reviews/reviews.jsonl

    Steps:
        1. Read reviews.jsonl from S3 using _read_jsonl_from_s3()
        2. Print the number of rows and columns
        3. Load into bronze.reviews using _load_to_bronze()

    Returns:
        pd.DataFrame: The reviews data.
    """
    df = _read_jsonl_from_s3("raw/reviews/reviews.jsonl")
    print(f"✅ bronze_group6.reviews loaded {df.shape[0]} rows, {df.shape[1]} columns")
    _load_to_bronze(df, table_name="reviews", if_exists="replace")
    return df


# ---------------------------------------------------------------------------
# Extract functions — Parquet datasets (partitioned)
# ---------------------------------------------------------------------------
def extract_clickstream() -> pd.DataFrame:
    """
    Extract clickstream events from S3 and load them into bronze.clickstream.

    Format: Partitioned Parquet (Snappy compressed)
    S3 prefix: raw/clickstream/
    Structure:
        raw/clickstream/dt=2026-02-05/part-00001.snappy.parquet
        raw/clickstream/dt=2026-02-05/part-00002.snappy.parquet
        raw/clickstream/dt=2026-02-06/part-00001.snappy.parquet
        ...

    This is a large dataset (~544k rows, 30 days, 28 columns).
    The data is partitioned by date (dt=YYYY-MM-DD) and split into
    multiple part files per day.

    Steps:
        1. Read all partitions using _read_partitioned_parquet_from_s3()
        2. Print the number of rows and columns
        3. Load into bronze.clickstream using _load_to_bronze()

    Returns:
        pd.DataFrame: The clickstream data.
    """
    # TODO: Same pattern, but use _read_partitioned_parquet_from_s3()
    # Note: pass a prefix (folder path), not a file key
    prefix = "raw/clickstream/"
    df= _read_partitioned_parquet_from_s3(prefix)
    print(f"✅ bronze_group6.clickstream loaded {df.shape[0]} rows, {df.shape[1]} columns")
    _load_to_bronze(df, table_name="clickstream")
    return df


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------
def extract_all() -> dict[str, pd.DataFrame]:
    """
    Run the full extraction of all sources into Bronze.

    Extracts 6 datasets across 3 formats:
        - 4 CSV files  (products, users, orders, order_line_items)
        - 1 JSONL file  (reviews)
        - 1 Partitioned Parquet dataset (clickstream)

    Returns:
        dict: A dictionary {table_name: DataFrame} for each extracted table.
    """
    print(f"\n{'='*60}")
    print(f"  🥉 EXTRACT → Bronze ({BRONZE_SCHEMA})")
    print(f"{'='*60}\n")

    results = {}

    # TODO: Call each extract_*() function and store the result in the dict
    # There are 6 functions to call: 4 CSV + 1 JSONL + 1 Parquet

    results: dict[str, pd.DataFrame] = {}

    # CSV files
    results["products"] = extract_products()
    results["users"] = extract_users()
    results["orders"] = extract_orders()
    results["order_line_items"] = extract_order_line_items()

    # JSONL file
    results["reviews"] = extract_reviews()

    # Partitioned Parquet dataset
    results["clickstream"] = extract_clickstream()

    print(f"\n  ✅ Extraction complete — {len(results)} tables loaded into {BRONZE_SCHEMA}")
    return results


# ---------------------------------------------------------------------------
# Entry point to test extraction alone
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    extract_all()
