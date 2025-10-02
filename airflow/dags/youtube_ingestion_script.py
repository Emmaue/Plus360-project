import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

SF_USER = os.getenv("SF_USER")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_ACCOUNT = os.getenv("SF_ACCOUNT")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE")
SF_DATABASE = os.getenv("SF_DATABASE")
SF_SCHEMA = os.getenv("SF_SCHEMA")


def ingest_csv_to_snowflake(file_path, table_name, pk_column, conn, full_refresh=False):
    """
    Load a CSV into Snowflake with safe upsert using staging + MERGE
    """
    df = pd.read_csv(file_path)
    df.columns = [col.upper() for col in df.columns]

    cur = conn.cursor()

    # Optionally truncate table if full refresh
    if full_refresh:
        cur.execute(f"TRUNCATE TABLE IF EXISTS {SF_DATABASE}.{SF_SCHEMA}.{table_name}")
        print(f"🧹 Table {SF_SCHEMA}.{table_name} truncated before reload")

    # Create staging temp table
    temp_table = f"{table_name}_STAGING"
    cur.execute(f"""
        CREATE OR REPLACE TEMP TABLE {SF_DATABASE}.{SF_SCHEMA}.{temp_table}
        LIKE {SF_DATABASE}.{SF_SCHEMA}.{table_name}
    """)

    # Write data into staging table
    success, nrows, nchunks, _ = write_pandas(
        conn, df, table_name=temp_table, database=SF_DATABASE, schema=SF_SCHEMA
    )
    print(f"📥 Loaded {nrows} rows into staging table {temp_table}")

    # Build dynamic MERGE statement
    merge_sql = f"""
    MERGE INTO {SF_DATABASE}.{SF_SCHEMA}.{table_name} t
    USING {SF_DATABASE}.{SF_SCHEMA}.{temp_table} s
    ON t.{pk_column.upper()} = s.{pk_column.upper()}
    WHEN MATCHED THEN UPDATE SET {", ".join([f"t.{col} = s.{col}" for col in df.columns if col.upper() != pk_column.upper()])}
    WHEN NOT MATCHED THEN INSERT ({", ".join(df.columns)})
    VALUES ({", ".join([f"s.{col}" for col in df.columns])})
    """
    cur.execute(merge_sql)
    print(f"✅ Merge completed: {table_name} updated/inserted {nrows} rows")


def main():
    conn = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
    )

    # Define ingestion mapping (file → table → primary key)
    files_to_ingest = [
        ("/opt/airflow/files/youtube_ad_revenue.csv", "YOUTUBE_AD_REVENUE_RAW", "AD_ID"),
        ("/opt/airflow/files/youtube_content.csv", "YOUTUBE_CONTENT_RAW", "CONTENT_ID"),
        ("/opt/airflow/files/youtube_engagement.csv", "YOUTUBE_ENGAGEMENT_RAW", "ENGAGEMENT_ID"),
    ]

    for file_path, table_name, pk_column in files_to_ingest:
        ingest_csv_to_snowflake(file_path, table_name, pk_column, conn, full_refresh=False)

    conn.close()


if __name__ == "__main__":
    main()
