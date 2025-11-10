# Data-Driven Content Strategy: Uncovering Engagement Insights for Pulse360 Media

## Business Problem
 This project addresses a critical business problem for Pulse360 Media, a digital content company facing a drop in user engagement and ad revenue. I designed and implemented a Modern Data Stack to consolidate content performance metrics from disparate sources (YouTube, Salesforce CRM, and Newsletters) into a centralized, analytical data warehouse. The end-to-end pipeline leverages ELT (Extract, Load, Transform) principles to deliver actionable insights on content performance, optimal publishing schedules, and cross-platform synergy, empowering content and marketing teams to reverse the engagement decline.

 ## Project Goals
 - Extract data from PipeDrive CRM, YouTube API, and local newsletter CSVs.
 - Design and implement a Snowflake data warehouse with dbt for transformations.
 - Perform incremental loads and handle large-scale data (500,000+ rows).
 - Set up partitioning and indexing in Snowflake for optimized querying.
 - Create a Power BI dashboard to visualize engagement insights.
 - Orchestrate the pipeline using Apache Airflow and send daily email notifications.

## Data Sources
 - YouTube Data: Pull video title, views, likes, comments, publish date using simulated YouTube Data.
 - PipeDrive: Simulate CRM campaign data with fields like sent_date, open_rate, click_rate.
 - Local File: Newsletter engagement logs (CSV/Excel) with fields like date_sent, open_status,
 clicks.

 ## Tools & Technologies
 - Python – for data ingestion
 - Snowflake – data warehouse
 - dbt – for transformation and incremental models
 - Power BI – for dashboards
 - Apache Airflow – for orchestration and alerts
 - Git – for version control with team-like workflow
 - YouTube Data – for real-time engagement data
 - Faker – for simulating CRM and newsletter data

## Data Warehouse Design & Modeling
This project utilizes a Star Schema to structure the analytical data in Snowflake, ensuring efficient querying and reporting. The modeling was primarily implemented using dbdiagram.
<img width="732" height="601" alt="image" src="https://github.com/user-attachments/assets/bb268732-bfd3-476d-8ea9-0f7de1fbacbb" />

The data catalog
| Table Name                    | Type          | Description                                                                                                                     |
| ----------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| **Engagement_Fact**           | Fact          | Contains key metrics (views, likes, comments, clicks) aggregated by content, platform, and time.                                |
| **Content_exposure_factless** | Factless Fact | Tracks when content was posted and which customer was exposed to it, without any measure. It links all key dimensions together. |
| **Ad_revenue_Fact**           | Fact          | Tracks ad-related events like clicks and associated revenue.                                                                    |
| **Subscription_Fact**         | Fact          | Tracks customer subscription activity and payment status.                                                                       |
| **Time**                      | Dimension     | Date and time components for analysis (e.g., hour, day, month).                                                                 |
| **Engagement_date**           | Dimension     | Date details for time-series analysis.                                                                                          |
| **Content**                   | Dimension     | Details about the content itself (Type, Posting Date/Time).                                                                     |
| **Platform**                  | Dimension     | The source platform (YouTube, Newsletter, Social).                                                                              |
| **Customers**                 | Dimension     | Customer attributes (Age, Location, Segment).                                                                                   |
| **Product_Plan**              | Dimension     | Details of the product/subscription plans.                                                                                      |
## Snowflake Data Warehouse Setup
Snowflake was chosen as the central cloud data warehouse for its scalability, performance, and separation of compute and storage.

### Schema Architecture
The data pipeline utilizes a layered architecture within the PULSE360_DB database to enforce data governance and clear separation between data stages:

RAW_SCHEMA: Landing zone for the initial, unprocessed data extracted by Airbyte.

STAGING_SCHEMA: Intermediate layer where raw data is cleaned, standardized, and de-duplicated.

ANALYTICS_SCHEMA: The final, transformed data layer containing the dbt-modeled star schema, ready for consumption by Power BI.

<img width="1280" height="610" alt="image" src="https://github.com/user-attachments/assets/1e199bc5-32a2-4baf-8823-aa3cc22dbae1" />
- Create layered schemas create schema raw_schema; create schema staging_schema; create schema Analytics_schema;
- Set up role-based access control (RBAC) create role data_engineer; create user Emmanuel; grant privileges on database Pulse360_DB to role data_engineer; -- ... and grants on specific schemas.

## Orchestration & Development Environment
Apache Airflow and Docker were used to orchestrate the end-to-end data pipeline, ensuring reliable and scheduled execution of ELT jobs.

### Local Development Setup (VS Code & Docker Compose)
The entire development and orchestration environment was containerized using Docker Compose for reproducibility and portability.
<img width="1280" height="648" alt="image" src="https://github.com/user-attachments/assets/ec94ec82-6ff3-4c62-abd4-78bcbc1e0055" />

- Airflow: Orchestrates the DAGs (Directed Acyclic Graphs) that sequence the Airbyte syncs, dbt transformations, and PostgreSQL loads.
- PostgreSQL: Used as both the Airflow Metadata Database and a dedicated Analytical Database to serve the Power BI dashboard (optimized via the Partitioning and Indexing step).
- Redis: Serves as the Airflow Redis broker.
- The docker-compose.yml file sets up key services like Postgres (to serve Airflow's metadata and act as the optimized analytical database) and Redis for efficient queue management. This setup ensures a local, fully functional data stack for testing and development.
- Github: serves the versoning purpose for the project.

## Data Ingestion (Extract & Load)
I leveraged Custom Python Scripts for data ingestion for sources like CRM data, YouTube and local Files data demonstrating flexibility in data extraction.

A. Ingesting CRM Data (Pipedrive Simulation)
Since the CRM data (simulating Pipedrive) required complex pagination and API handling, a custom Python script was developed.

The CRM data in PipeDrive before ingestion (N.B: Every information here doesn't represent real human infos but simulated using Faker)
<img width="958" height="483" alt="image" src="https://github.com/user-attachments/assets/da41d8c2-1e3a-4330-b744-18b821cf6ee6" />


Technology: Python (using requests for API interaction and snowflake-connector-python with write_pandas for bulk loading).

Process: The script handles API pagination to pull large volumes of contact records (10K+ rows) and loads the raw, nested JSON/Variant object directly into the RAW_CONTACTS table.
```
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
```

The initial load places the entire contact record into a single VARIANT column named CONTACT for storage efficiency and schema flexibility.
<img width="979" height="536" alt="image" src="https://github.com/user-attachments/assets/eb548a43-afb3-4069-9f3e-b13950f4a3bb" />

After ingestion, the nested CRM JSON/VARIANT data was transformed into a clean, relational table (CONTACTS_CLEAN) using Snowflake's native JSON parsing functions and the LATERAL FLATTEN clause. This step extracted critical fields like Customer_ID, first_name, phone, and derived fields like customer_status.
```
CREATE OR REPLACE TABLE CONTACTS_CLEAN AS
SELECT
    CONTACT:primary_email::string            AS Customer_ID,
    ...
    PHONE.value:value::string               AS phone,
    IFF(CONTACT:active_flag::boolean, 'Active', 'Prospect') AS customer_status
FROM RAW_CONTACTS,
     LATERAL FLATTEN(INPUT => CONTACT:phone) AS PHONE;
```
CRM data now cleaned
<img width="980" height="423" alt="image" src="https://github.com/user-attachments/assets/6bf47a81-8ae8-4f6f-963b-d09ffb1cc18d" />

2. YouTube content and engagement metrics, along with local Newsletter files, were pulled and landed directly into the RAW_SCHEMA tables like YOUTUBE_CONTENT_RAW.

All tables ingested now on Snowflake
<img width="1280" height="567" alt="image" src="https://github.com/user-attachments/assets/e5dbdc45-966b-4e3f-aa8d-0c42b91c3fb2" />

## dbt Transformation and Data Modeling Structure
dbt (data build tool) was essential for managing the project's complex transformations, enforcing testing, documentation, and utilizing best practices for data warehousing.

### dbt Project Structure and Profiles
The project adheres to the standard dbt best practice of layered modeling:

- staging: Cleans and standardizes raw data (e.g., column renaming, type casting, basic coalescing). Models here are typically views (materialized='view').

- intermediate (int): Performs complex business logic, de-duplication, and joining of staging tables to prepare for the final mart layer.

- mart: Contains the final, user-facing analytical models, primarily the Star Schema fact and dimension tables, ready for consumption by Power BI.

- Development Profiles: A profiles.yml file was configured to support distinct dev and prod environments. This ensures that development and testing (e.g., using a smaller compute warehouse or different schemas) can occur safely without impacting the production analytical tables used by the business dashboard.

### Transformation Examples
1. Staging Layer: Standardization (stg_youtube_content)
This initial layer focuses on basic cleanup, ensuring column integrity, and handling null values using COALESCE.

- Action: Transforms the raw YouTube content data, ensuring key columns like TITLE and CHANNEL_NAME are standardized.

- Materialization: Defined as a view to avoid duplicating raw data.

Key SQL Logic:
```
{{ config(materialized='view') }}

with Youtube_content as (
    select
        {{ preserve_pk('CONTENT_ID') }} as Content_id,
        coalesce(CHANNEL_NAME, 'Unknown Channel') as Channel_name,
        coalesce(TITLE, 'Untitled') as Title,
        coalesce(UPLOAD_DATE, cast(null as date)) as Upload_date,
        coalesce(DURATION_SECONDS, 0) as Duration_seconds,
        coalesce(CATEGORY, 'Uncategorized') as Category
    from pulse360_db.raw_schema.YOUTUBE_CONTENT_RAW
)
select
    Content_id,
    Channel_name,
    Title,
    Upload_date,
    Duration_seconds,
    Category
from Youtube_content
```

2. Intermediate Layer: Business Logic & De-duplication (int_customers)
The intermediate layer performs complex operations, such as handling slowly changing dimensions, de-duplication, and complex data type manipulation.

- Action: De-duplicates customer records (from the cleaned CRM data) using a Window Function (row_number()) and standardizes fields like full_name using INITCAP and TRIM.

- Key SQL Logic: Uses FIRST_VALUE to pick the earliest known phone number and row_number() to select the latest/most reliable customer record (WHERE rn = 1).

```
{{ config(materialized ='view') }}

with base as (
SELECT
    CUSTOMER_ID,
    concat(FIRST_NAME, ' ', LAST_NAME) AS full_name,
    FIRST_VALUE(phone) OVER (PARTITION BY customer_id ORDER BY signup_date) AS phone,
    signup_date,
    country,
    'crm' as source_system,
    row_number() over (partition by customer_id order by signup_date desc) as rn,
    customer_status
FROM
    {{ ref('stg_customers') }}
),

deduped as(
    Select
    CUSTOMER_ID,
    initcap(trim(full_name)) as full_name,
    phone,
    CASE
        WHEN signup_date < to_date('1900-01-01') or signup_date is null THEN to_date('1900-01-01')
        ELSE signup_date
    END AS signup_date,
    country,
    source_system,
    rn,
    customer_status
FROM base
WHERE rn = 1
)

Select
    CUSTOMER_ID,
    full_name,
    phone,
    signup_date,
    country,
    source_system,
    customer_status
from deduped
```

3. Mart Layer: Incremental Fact Loading (fact_engagement)
The final mart layer builds the Engagement_Fact table, which is optimized for analytics.

- Materialization: Configured as incremental using the merge strategy, keyed on engagement_pk.

- Purpose: This critical strategy ensures that only new engagement events are processed and added to the fact table during daily Airflow runs, fulfilling the project goal of efficiently handling large-scale data and reducing data processing time and cost.

- Logic: Joins intermediate engagement events with the dimension tables (dim_content, dim_customers, dim_date) to build the final star schema fact table.

```
{{ config(
    materialized='incremental',
    unique_key='engagement_pk',
    incremental_strategy='merge'
) }}

with base as (
    select
        e.engagement_pk,
        c.content_pk,
        c.content_id,
        cust.customer_sk,
        d.date_sk,
        e.platform,
        e.event_date,
        e.source_system,

        -- metrics
        e.opens_count,
        e.clicks_count,
        e.likes_count,
        e.comments_count,
        e.shares_count,
        e.watch_time_seconds

    from {{ ref('int_engagement_events') }} e
    left join {{ ref('dim_content_table') }}  c
        on e.content_id = c.content_id
    left join {{ ref('dim_customers') }} cust
        on e.customer_id = cust.customer_id
    left join {{ ref('dim_date_table') }} d
        on e.date_key = d.date_sk

)


select * from base
```
dbt run to set up all dev schema
<img width="1040" height="561" alt="image" src="https://github.com/user-attachments/assets/283d27c4-9e77-49f2-8d5b-ddf4c20f28ad" />

dbt run for prod schemas
<img width="1020" height="522" alt="image" src="https://github.com/user-attachments/assets/34e046eb-eece-4797-8554-28e3bbdb6ad7" />

dbt test was also done to cheeck for data integrity and other constraints
<img width="995" height="507" alt="image" src="https://github.com/user-attachments/assets/da1a7f83-0a09-4ff2-b025-a381d4b10ab8" />

dbt dev tables after modeling in Snowflake
<img width="318" height="512" alt="image" src="https://github.com/user-attachments/assets/09414fc4-015c-4882-8f50-5ab36070f035" />

prod tables in snowflake
<img width="311" height="520" alt="image" src="https://github.com/user-attachments/assets/129cd453-eca5-4715-9b22-1591a4073e7f" />

dbt documentation for data cataloging
<img width="1014" height="492" alt="image" src="https://github.com/user-attachments/assets/ffd8f88c-fe44-4a6e-a376-0fe4b910ff0e" />

dbt doc lineage graph
<img width="1280" height="543" alt="image" src="https://github.com/user-attachments/assets/b710cf98-b36e-410b-84b8-70e480f81d36" />














