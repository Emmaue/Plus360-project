from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import youtube_ingestion_script  # renamed ingestion script

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
    description="Ingest YouTube CSVs into Snowflake"
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_youtube_csvs",
        python_callable=youtube_ingestion_script.main
    )
