import os
import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv


# ================================
# LOAD ENV VARIABLES
# ================================
load_dotenv()

# Pipedrive
API_KEY = os.getenv("API_KEY")
COMPANYDOMAIN = os.getenv("COMPANYDOMAIN")
ENDPOINT = f"https://{COMPANYDOMAIN}.pipedrive.com/api/v1/persons"

# Snowflake
SF_USER = os.getenv("SF_USER")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_ACCOUNT = os.getenv("SF_ACCOUNT")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE")
SF_DATABASE = os.getenv("SF_DATABASE")
SF_SCHEMA = os.getenv("SF_SCHEMA")
SF_TABLE = os.getenv("SF_TABLE")


# ================================
# FETCH CONTACTS FROM PIPEDRIVE
# ================================
def fetch_contacts():
    all_contacts = []
    start, limit = 0, 500
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
    return pd.DataFrame({"CONTACT": all_contacts})


# ================================
# CONNECT TO SNOWFLAKE
# ================================
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
    )


# ================================
# MAIN INGESTION PROCESS
# ================================
def main():
    # Fetch data
    df = fetch_contacts()
    if df.empty:
        raise Exception("❌ No contacts pulled from Pipedrive API")

    print(f"📊 DataFrame created with {len(df)} rows")

    # Connect to Snowflake
    print("🔗 Connecting to Snowflake...")
    conn = get_snowflake_connection()
    print("✅ Connected to Snowflake")

    # Ensure table exists
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE} (
        CONTACT VARIANT
    );
    """
    conn.cursor().execute(create_table_sql)
    print("✅ Table checked/created")

    # Load data
    print("⬆️ Loading into Snowflake...")
    success, nchunks, nrows, _ = write_pandas(
        conn, df, table_name=SF_TABLE, database=SF_DATABASE, schema=SF_SCHEMA
    )

    print(f"✅ Loaded {nrows} rows into {SF_SCHEMA}.{SF_TABLE} (Success={success})")

    conn.close()


if __name__ == "__main__":
    main()
