from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

def run_ingestion():
    import youtube_ingestion_script
    youtube_ingestion_script.main()

with DAG(
    "youtube_ingestion_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Ingest YouTube CSVs into Snowflake"
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_youtube_csvs",
        python_callable=run_ingestion
    )