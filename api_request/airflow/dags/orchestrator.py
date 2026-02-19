import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta


sys.path.append("/opt/airflow/dags")


def safe_main_callable():
    from insert_records import main
    return main()

default_args = {
    "description":"A DAG TO orchestrate data",
    "start_date":datetime(2026, 2, 16),
    "catchup":False,
    "retries":2,
    "retry_delay":timedelta(minutes=5),
    "email": "alexanders7sg@gmail.com",  
    "email_on_failure": True,
    "email_on_retry": False,

}
dag = DAG(
    dag_id = "weather-orchestator",
    default_args=default_args,
    schedule = timedelta(minutes=5)

)

with dag:
    task1 = PythonOperator(
        task_id = "ingest_data",
        python_callable = safe_main_callable
    )

