from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date":datetime(year=2019, month=9, day=14),
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

test = "test"

dag = DAG("Testing", default_args=default_args, schedule_interval='10 10 * * *')

 
t1 = BashOperator(task_id="Gettingrankings1", bash_command="echo this is a " + test , dag=dag)