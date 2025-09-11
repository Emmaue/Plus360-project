import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector

# "C:\Users\HP\Documents\Plus360 project\Files\youtube_ad_revenue.csv"
# "C:\Users\HP\Documents\Plus360 project\Files\youtube_content.csv"
# "C:\Users\HP\Documents\Plus360 project\Files\youtube_engagement.csv"


def ingest_csv_to_snowflake(file_path, table_name, conn, database="PULSE360_DB", schema="RAW_SCHEMA"):
    # Read CSV
    df = pd.read_csv(file_path)

    df.columns = [col.upper() for col in df.columns]

    success, nrows, nchunks, _ = write_pandas(
        conn,
        df,
        table_name=table_name,
        database=database,
        schema=schema
    )

    print(f"Loaded {nrows} rows into {schema}.{table_name} (success={success})")

  

conn = snowflake.connector.connect(
    user='Emmanuel',
    account='GVBQDXI-UC20219',
    password='Iamemmanueljustice@1',
    warehouse='PULSE_MEDIA',
    database='PULSE360_DB',
    schema='RAW_SCHEMA'
)


files_to_ingest = {
    r"C:\Users\HP\Documents\Plus360 project\Files\youtube_ad_revenue.csv": "YOUTUBE_AD_REVENUE_RAW",
    r"C:\Users\HP\Documents\Plus360 project\Files\youtube_content.csv": "YOUTUBE_CONTENT_RAW",
    r"C:\Users\HP\Documents\Plus360 project\Files\youtube_engagement.csv": "YOUTUBE_ENGAGEMENT_RAW"
}

for file_path, table_name in files_to_ingest.items():
    ingest_csv_to_snowflake(file_path,table_name, conn)



