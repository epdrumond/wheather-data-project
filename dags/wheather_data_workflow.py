from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

from datetime import datetime

from includes.wheather_utils import extract_wheather_data, load_wheather_data

DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1)
}
default_extraction_args = {
    "start_date": "2024-01-03",
    "end_date": "2024-01-04"
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

load_data = PythonOperator(
    task_id="load_data",
    python_callable=load_wheather_data,
    op_kwargs={
        "source_dataset": "src",
        "source_wheather_table": "src_wheather",
        "source_stations_table": "src_stations"
    },
    dag=dag
)

transform_data = BashOperator(
    task_id="transform_data",
    bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag
)

extract_data >> load_data 
load_data >> transform_data