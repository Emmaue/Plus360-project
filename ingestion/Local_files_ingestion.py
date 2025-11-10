import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas





df = pd.read_csv("C:/Users/HP/Documents/Plus360 project/Files/time_dim.csv")
    # df6 = pd.read_csv("C:/Users/HP/Documents/Plus360 project/Files/date_dim_full_2020_2025.csv")


df.columns = [col.upper() for col in df.columns]

conn = snowflake.connector.connect(
SF_USER = os.getenv("SF_USER")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_ACCOUNT = os.getenv("SF_ACCOUNT")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE")
SF_DATABASE = os.getenv("SF_DATABASE")
SF_SCHEMA = os.getenv("SF_SCHEMA")
SF_TABLE = os.getenv("SF_TABLE")
)

success, nrows, nchunks, _ =write_pandas(
    conn,
    df,
    database= 'PULSE360_DB',
    schema= 'RAW_SCHEMA',
    table_name= 'TIME_DIM'
)

print(f'Loaded{nrows} into the table. success={success}')
