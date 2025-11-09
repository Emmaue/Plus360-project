import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def get_snowflake_credentials():
    """Get Snowflake credentials from environment variables"""
    return {
        'user': os.getenv("SF_USER"),
        'password': os.getenv("SF_PASSWORD"),
        'account': os.getenv("SF_ACCOUNT"),
        'warehouse': os.getenv("SF_WAREHOUSE"),
        'database': os.getenv("SF_DATABASE"),
        'schema': os.getenv("SF_SCHEMA")
    }


def ingest_csv_to_snowflake(file_path, table_name, pk_column, conn, sf_database, sf_schema, full_refresh=False):
    """
    Load a CSV into Snowflake with safe upsert using staging + MERGE
    """
    df = pd.read_csv(file_path)
    df.columns = [col.upper() for col in df.columns]

    cur = conn.cursor()

    # Optionally truncate table if full refresh
    if full_refresh:
        cur.execute(f"TRUNCATE TABLE IF EXISTS {sf_database}.{sf_schema}.{table_name}")
        print(f"🧹 Table {sf_schema}.{table_name} truncated before reload")

    # Create staging temp table
    temp_table = f"{table_name}_STAGING"
    cur.execute(f"""
        CREATE OR REPLACE TEMP TABLE {sf_database}.{sf_schema}.{temp_table}
        LIKE {sf_database}.{sf_schema}.{table_name}
    """)

    # Write data into staging table
    success, nrows, nchunks, _ = write_pandas(
        conn, df, table_name=temp_table, database=sf_database, schema=sf_schema
    )
    print(f"📥 Loaded {nrows} rows into staging table {temp_table}")

    # Build dynamic MERGE statement
    merge_sql = f"""
    MERGE INTO {sf_database}.{sf_schema}.{table_name} t
    USING {sf_database}.{sf_schema}.{temp_table} s
    ON t.{pk_column.upper()} = s.{pk_column.upper()}
    WHEN MATCHED THEN UPDATE SET {", ".join([f"t.{col} = s.{col}" for col in df.columns if col.upper() != pk_column.upper()])}
    WHEN NOT MATCHED THEN INSERT ({", ".join(df.columns)})
    VALUES ({", ".join([f"s.{col}" for col in df.columns])})
    """
    cur.execute(merge_sql)
    print(f"✅ Merge completed: {table_name} updated/inserted {nrows} rows")


def main():
    """Main function to run the ingestion process"""
    # Get credentials only when function is called
    creds = get_snowflake_credentials()
    
    conn = snowflake.connector.connect(
        user=creds['user'],
        password=creds['password'],
        account=creds['account'],
        warehouse=creds['warehouse'],
        database=creds['database'],
        schema=creds['schema'],
    )

    # Define ingestion mapping (file → table → primary key)
    files_to_ingest = [
        ("/opt/airflow/files/youtube_ad_revenue.csv", "YOUTUBE_AD_REVENUE_RAW", "AD_ID"),
        ("/opt/airflow/files/youtube_content.csv", "YOUTUBE_CONTENT_RAW", "CONTENT_ID"),
        ("/opt/airflow/files/youtube_engagement.csv", "YOUTUBE_ENGAGEMENT_RAW", "ENGAGEMENT_ID"),
    ]

    for file_path, table_name, pk_column in files_to_ingest:
        ingest_csv_to_snowflake(
            file_path, 
            table_name, 
            pk_column, 
            conn, 
            creds['database'],
            creds['schema'],
            full_refresh=False
        )

    conn.close()
    print("✨ All files ingested successfully!")


if __name__ == "__main__":
    main()