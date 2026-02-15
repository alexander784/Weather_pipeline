import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta


sys.path.append("/opt/airflow/dags")


def safe_main_callable():
    from insert_records import main
    return main()



def example_task():
    print("This an example task")

default_args = {
    "description":"A DAG TO orchestrate data",
    "start_date":datetime(2026, 2, 14),
    "catchup":False,
    "retries":3,
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
        task_id = "indgest_data",
        python_callable = safe_main_callable
    )

