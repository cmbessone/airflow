from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="example_dag",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    description="Dag de ejemplo para validar entorno Airflow",
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> end
