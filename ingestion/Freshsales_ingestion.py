import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas





df = pd.read_csv("C:/Users/HP/Documents/Plus360 project/Files/time_dim.csv")
    # df6 = pd.read_csv("C:/Users/HP/Documents/Plus360 project/Files/date_dim_full_2020_2025.csv")


df.columns = [col.upper() for col in df.columns]

conn = snowflake.connector.connect(
    user = 'EMMANUEL',
    account = 'GVBQDXI-UC20219',
    warehouse = 'Pulse_media',
    database = 'pulse360_db',
    password = 'Iamemmanueljustice@1',
    schema = 'raw_schema'
)

success, nrows, nchunks, _ =write_pandas(
    conn,
    df,
    database= 'PULSE360_DB',
    schema= 'RAW_SCHEMA',
    table_name= 'TIME_DIM'
)

print(f'Loaded{nrows} into the table. success={success}')
