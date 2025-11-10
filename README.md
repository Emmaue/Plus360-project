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





