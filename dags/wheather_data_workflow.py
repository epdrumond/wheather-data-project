from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def test():
    print("Edilson")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1)
}

dag = DAG(
    dag_id="wheather_dag",
    default_args=default_args,
    schedule_interval="0 0 * * 1",
    catchup=False
)

extract_wheather_data = PythonOperator(
    task_id="extract_wheather_data",
    python_callable=test,
    dag=dag
)
