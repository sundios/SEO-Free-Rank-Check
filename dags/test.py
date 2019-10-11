from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import os
import airflow.hooks.S3_hook

def upload_file_to_S3_with_hook(filename, key, bucket_name, replace):
    hook = airflow.hooks.S3_hook.S3Hook('my_conn_S3')
    hook.load_file(filename, key, bucket_name,replace)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date":datetime(year=2019, month=9, day=22),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("S3_test", default_args=default_args, schedule_interval='10 10 * * *')

t1 = BashOperator(task_id="test", bash_command="echo 'Hola'", dag=dag)

t2 = PythonOperator(
    task_id='upload_to_S3',
    python_callable=upload_file_to_S3_with_hook,
    op_kwargs={
        'filename': '/usr/local/airflow/dags/kw1/kw1.csv',
        'key': 'kw1/kw1.csv',
        'bucket_name': 'bucket-test85',
        'replace':'True'
    },
    dag=dag)

t1 >> t2