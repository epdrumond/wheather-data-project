from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from includes.wheather_utils import extract_wheather_data

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1)
}
default_extraction_args = {
    "start_date": "2024-01-01",
    "end_date": "2024-01-02"
}

dag = DAG(
    dag_id="wheather_dag",
    default_args=default_args,
    schedule_interval="0 0 * * 1",
    catchup=False
)

extract_data = PythonOperator(
    task_id="extract_data",
    python_callable=extract_wheather_data,
    op_kwargs={
        "start_date": default_extraction_args["start_date"],
        "end_date": default_extraction_args["end_date"]
    },
    dag=dag
)
