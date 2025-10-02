from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="hello_world",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    
    task1 = BashOperator(
        task_id="print_hello",
        bash_command="echo 'Hello Plus360 Media Pipeline!'"
    )