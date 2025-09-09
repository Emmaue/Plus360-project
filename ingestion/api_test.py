import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import json

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
    try:
        params = {"api_token": API_KEY, "start": start, "limit": limit}
        response = requests.get(ENDPOINT, params=params)

        print(f"➡️ Requesting batch starting at {start}, status {response.status_code}")

        if response.status_code != 200:
            print(f"❌ API call failed: {response.status_code}, {response.text}")
            break

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
        
    except Exception as e:
        print(f"❌ Error during API call: {e}")
        break

print(f"✅ Finished pulling {len(all_contacts)} contacts")

# ================================
# PREPARE DATAFRAME
# ================================
if not all_contacts:
    raise Exception("❌ No contacts pulled from Pipedrive API")

# Convert to JSON strings for VARIANT column
contacts_json = [json.dumps(contact) for contact in all_contacts]
df = pd.DataFrame({"CONTACT": contacts_json})
print(f"📊 DataFrame created with {len(df)} rows")

# ================================
# CONNECT TO SNOWFLAKE
# ================================
print("🔗 Connecting to Snowflake...")
try:
    conn = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
    )
    print("✅ Connected to Snowflake")
except Exception as e:
    print(f"❌ Failed to connect to Snowflake: {e}")
    exit(1)

# ================================
# ENSURE TABLE EXISTS
# ================================
try:
    cursor = conn.cursor()
    
    # Drop table if it exists (optional - for clean slate)
    drop_table_sql = f"DROP TABLE IF EXISTS {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE};"
    cursor.execute(drop_table_sql)
    print("🗑️ Dropped existing table (if any)")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE} (
        CONTACT VARIANT,
        LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()  # Important: commit the DDL
    print("✅ Table checked/created")
    
except Exception as e:
    print(f"❌ Error creating table: {e}")
    conn.close()
    exit(1)

# ================================
# LOAD DATA
# ================================
print("⬆️ Loading into Snowflake...")
try:
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name=SF_TABLE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        overwrite=True  # This ensures clean data load
    )
    
    # CRITICAL: Commit the transaction
    conn.commit()
    print(f"✅ write_pandas completed: Success={success}, Rows={nrows}, Chunks={nchunks}")
    
except Exception as e:
    print(f"❌ Error during write_pandas: {e}")
    conn.rollback()
    conn.close()
    exit(1)

# ================================
# VERIFY DATA WAS LOADED
# ================================
print("🔍 Verifying data in Snowflake...")
try:
    cursor = conn.cursor()
    
    # Count rows in the table
    count_sql = f"SELECT COUNT(*) FROM {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE};"
    cursor.execute(count_sql)
    row_count = cursor.fetchone()[0]
    print(f"📊 Table contains {row_count} rows")
    
    # Sample a few rows to verify content
    sample_sql = f"SELECT CONTACT FROM {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE} LIMIT 3;"
    cursor.execute(sample_sql)
    sample_rows = cursor.fetchall()
    
    print("📋 Sample data:")
    for i, row in enumerate(sample_rows, 1):
        contact_data = json.loads(row[0]) if row[0] else {}
        contact_name = contact_data.get('name', 'No name')
        contact_email = contact_data.get('email', [{}])[0].get('value', 'No email') if contact_data.get('email') else 'No email'
        print(f"   Row {i}: {contact_name} ({contact_email})")
    
    if row_count == len(all_contacts):
        print("✅ SUCCESS: All data loaded correctly!")
    else:
        print(f"⚠️ WARNING: Expected {len(all_contacts)} rows, but found {row_count}")
        
except Exception as e:
    print(f"❌ Error verifying data: {e}")

# ================================
# CLEANUP
# ================================
try:
    conn.close()
    print("🔗 Connection closed")
except:
    pass

print("🎉 ETL process completed!")