from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import youtube_ingestion_script

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    "youtube_ingestion_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Ingest YouTube CSVs into Snowflake and run dbt models"
) as dag:

    # Task 1: Ingest CSV data to Snowflake
    ingest_task = PythonOperator(
        task_id="ingest_youtube_csvs",
        python_callable=youtube_ingestion_script.main
    )

    # Task 2: Run dbt deps (install dependencies)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt && dbt deps --profiles-dir .",
    )

    # Task 3: Run dbt models in the intermediate folder
    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir . --select models/intermediate/*",
    )

    # Task 4: Run dbt models in the mart folder
    dbt_run_mart = BashOperator(
        task_id="dbt_run_mart",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir . --select models/mart/*",
    )

    # Task 5: Run dbt models in the staging folder
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir . --select models/staging/*",
    )

    # Task 6: Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir .",
    )

    # Define task dependencies
    # First ingest data, then run dbt deps, then staging -> intermediate -> mart, finally test
    ingest_task >> dbt_deps >> dbt_run_staging >> dbt_run_intermediate >> dbt_run_mart >> dbt_test