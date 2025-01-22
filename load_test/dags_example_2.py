import logging
import random
import datetime
from datetime import timedelta
import string
from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator



YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": YESTERDAY,
}

def generate_random_string(ti):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(1024*256)) # 1K
    ti.xcom_push(key="random_string-"+ti.run_id, value=result_str)
    return result_str

with models.DAG(
    "example_2",
    catchup=False,
    default_args=default_args,
    schedule=timedelta(seconds=1),
) as dag:
    start_task = DummyOperator(task_id='start_task', dag=dag)
    for i in range(100):    
        gen_string_task = PythonOperator(
            task_id=f"generate_random_string_{i}", python_callable=generate_random_string
        )
        start_task >> gen_string_task
    