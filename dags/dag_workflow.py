from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime , timedelta

from tasks.task1 import extract_func
from tasks.task2 import load_raw_func
from tasks.task3 import clean_func
from tasks.task4 import insert_func

default_args = {
    'owner' : 'LamPhuc',
    'depends_on_past' : False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5)
}

with DAG(
    'crypto_pipeline',
    default_args = default_args,
    schedule_interval = '@daily',
    start_date = datetime(2025,9,12),
    catchup = False
) as dag :
    
    task1 = PythonOperator(
        task_id='extract_json',
        python_callable=extract_func
    )

    task2 = PythonOperator(
        task_id='load_raw',
        python_callable=load_raw_func
    )

    task3 = PythonOperator(
        task_id='clean_data',
        python_callable=clean_func
    )

    task4 = PythonOperator(
        task_id='insert_mart',
        python_callable=insert_func
    )

    task1 >> task2 >> task3 >> task4