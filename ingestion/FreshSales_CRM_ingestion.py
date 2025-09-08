import pandas as pd
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Call Freshsales api
api_Key = 'eSy__-Ji1BRzZGASDTFBPQ'
domain = 'pqpmarketingconsult.myfreshworks.com'
endpoint = f"https://{domain}/crm/sales/api/contacts"



Headers = {
    "Authorization": f"Token token={api_Key}",
    "Content-Type": "application/json"
}

response =requests.get(endpoint, headers=Headers)

# check response
if response.status_code != 200:
    raise Exception(f'Api call failed:{response.status_code}, {response.text}')
 
# Parse Json into dataframe
data = response.json()
contacts = data.get("contacts",[])
df = pd.DataFrame(contacts)

df["CONTACT"] = df.to_dict(orient="records")

conn = snowflake.connector.connect(
    user = 'EMMANUEL',
    account = 'GVBQDXI-UC20219',
    warehouse = 'Pulse_media',
    database = 'pulse360_db',
    password = 'Iamemmanueljustice@1',
    schema = 'raw_schema'
)




success,nrows, nchunks, _, = write_pandas(
    conn,
    df[["CONTACT"]],
    database= 'PULSE360_DB',
    schema= 'RAW_SCHEMA',
    table_name= 'RAW_CONTACT'
)

print(f'loaded{nrows} to target table. Success{success}')

