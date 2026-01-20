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
    "start_date":datetime(2026, 1, 1),
    "catchup":False,
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