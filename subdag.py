#dags/subdag.py
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.bash_operator import BashOperator
from scripts import mysql_connector


# Dag is returned by a factory method
def sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
  dag_sub = DAG(
    '%s.%s' % (parent_dag_name, child_dag_name),
    schedule_interval=schedule_interval,
    start_date=start_date,
  )

  t0 = BashOperator(task_id="CopyingMasterCSVtoFile1", bash_command="cp ~/dags/kw*/*.csv ~/dags/all_kws" , dag=dag_sub)

  t1 = PythonOperator(task_id='pushing_toMysql', python_callable=mysql_connector.main ,dag=dag_sub)

  # t2 = MySqlOperator(
  #         task_id='basic_mysql',
  #         sql="show databases;",
  #         mysql_conn_id='rds_mysql',
  #         dag=dag_sub)

  t0 >> t1 
 

  return dag_sub