import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ================================
# PIPEDRIVE CONFIG
# ================================
API_KEY = "009b851b587d30a94de3957fb116c8211a7af2b4"
COMPANYDOMAIN = "pqpmarketingconsult"
ENDPOINT = f"https://{COMPANYDOMAIN}.pipedrive.com/api/v1/persons"

# ================================
# SNOWFLAKE CONFIG
# ================================
SF_USER = "Emmanuel"
SF_PASSWORD = "Iamemmanueljustice@1"
SF_ACCOUNT = "GVBQDXI-UC20219"
SF_WAREHOUSE = "PULSE_MEDIA"
SF_DATABASE = "PULSE360_DB"
SF_SCHEMA = "RAW_SCHEMA"
SF_TABLE = "RAW_CONTACTS"

# ================================
# PULL ALL CONTACTS (Pagination)
# ================================
all_contacts = []
start = 0
limit = 500
more_items = True

print("🚀 Starting API pull...")

while more_items:
    params = {"api_token": API_KEY, "start": start, "limit": limit}
    response = requests.get(ENDPOINT, params=params)

    print(f"➡️ Requesting batch starting at {start}, status {response.status_code}")

    if response.status_code != 200:
        raise Exception(f"API call failed: {response.status_code}, {response.text}")

    result = response.json()
    data = result.get("data", [])
    additional = result.get("additional_data", {})

    print(f"   ↳ Returned {len(data)} records")

    if data:
        all_contacts.extend(data)
    else:
        print("   ⚠️ No data returned in this batch, stopping.")
        break

    more_items = additional.get("pagination", {}).get("more_items_in_collection", False)
    start = additional.get("pagination", {}).get("next_start", 0)

print(f"✅ Finished pulling {len(all_contacts)} contacts")

# ================================
# PREPARE DATAFRAME
# ================================
if not all_contacts:
    raise Exception("❌ No contacts pulled from Pipedrive API")

df = pd.DataFrame({"CONTACT": all_contacts})
print(f"📊 DataFrame created with {len(df)} rows")

# ================================
# CONNECT TO SNOWFLAKE
# ================================
print("🔗 Connecting to Snowflake...")
conn = snowflake.connector.connect(
    user=SF_USER,
    password=SF_PASSWORD,
    account=SF_ACCOUNT,
    warehouse=SF_WAREHOUSE,
    database=SF_DATABASE,
    schema=SF_SCHEMA,
)
print("✅ Connected to Snowflake")

# ================================
# ENSURE TABLE EXISTS
# ================================
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE} (
    CONTACT VARIANT
);
"""
conn.cursor().execute(create_table_sql)
print("✅ Table checked/created")

# ================================
# LOAD DATA
# ================================
print("⬆️ Loading into Snowflake...")
success, nchunks, nrows, _ = write_pandas(
    conn,
    df,
    table_name=SF_TABLE,
    database=SF_DATABASE,
    schema=SF_SCHEMA,
)

print(f"✅ Loaded {nrows} rows into {SF_SCHEMA}.{SF_TABLE} (Success={success})")
