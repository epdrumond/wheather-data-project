from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

from datetime import datetime, timedelta

from includes.wheather_utils import extract_wheather_data, load_wheather_data

DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR")

reference_date = datetime.today() - timedelta(days=1)
start_date_str = (reference_date - timedelta(days=2)).strftime("%Y-%m-%d")
end_date_str = reference_date.strftime("%Y-%m-%d")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1)
}

    

with DAG(
    dag_id="wheather_dag",
    default_args=default_args,
    schedule_interval="0 0 * * 1",
    catchup=False,
    params={
        "start_date": start_date_str,
        "end_date": end_date_str
    }
) as dag:

    extract_data = PythonOperator( 
        task_id="extract_data",
        python_callable=extract_wheather_data,
        op_kwargs={
            "start_date": "{{ params.start_date }}",
            "end_date": "{{ params.end_date }}"
        }
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_wheather_data,
        op_kwargs={
            "source_dataset": "src",
            "source_wheather_table": "src_wheather",
            "source_stations_table": "src_stations"
        }
    )

    transform_data = BashOperator(
        task_id="transform_data",
        bash_command="dbt build --project-dir {} --profiles-dir {} --vars '{}'".format(
            DBT_PROJECT_DIR,
            DBT_PROFILES_DIR,
            "{start_date: '{{ params.start_date }}', end_date: '{{ params.end_date }}'}"
        ) 
    )

    extract_data >> load_data 
    load_data >> transform_data