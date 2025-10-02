import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SF_USER = os.getenv("SF_USER")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_ACCOUNT = os.getenv("SF_ACCOUNT")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE")
SF_DATABASE = os.getenv("SF_DATABASE")
SF_SCHEMA = os.getenv("SF_SCHEMA")

def ingest_csv_to_snowflake(file_path, table_name, conn):
    df = pd.read_csv(file_path)
    df.columns = [col.upper() for col in df.columns]

    success, nrows, nchunks, _ = write_pandas(
        conn, df, table_name=table_name, database=SF_DATABASE, schema=SF_SCHEMA
    )
    print(f"✅ Loaded {nrows} rows into {SF_SCHEMA}.{table_name} (success={success})")

def main():
    conn = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
    )

    # 👇 Updated paths to container mount location
    files_to_ingest = {
        "/opt/airflow/files/youtube_ad_revenue.csv": "YOUTUBE_AD_REVENUE_RAW",
        "/opt/airflow/files/youtube_content.csv": "YOUTUBE_CONTENT_RAW",
        "/opt/airflow/files/youtube_engagement.csv": "YOUTUBE_ENGAGEMENT_RAW"
    }

    for file_path, table_name in files_to_ingest.items():
        ingest_csv_to_snowflake(file_path, table_name, conn)

    conn.close()

if __name__ == "__main__":
    main()
