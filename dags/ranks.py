from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
from keywords import * 
from airflow.operators.subdag_operator import SubDagOperator
from subdag import sub_dag




PARENT_DAG_NAME = 'Keywords_check'
CHILD_DAG_NAME = 'Pushing_to_Mysql'


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date":datetime(year=2019, month=10, day=10),
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

#Main Dag

dag = DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='10 10 * * *',start_date=datetime(2018, 9, 27))

# Keyword 1 #

# running my script agains a url and the keywords I want to check in Google
KW1_t1 = BashOperator(task_id="Gettingrankings1", bash_command="python ~/dags/kw1/scripts/rank.py" + " " + url + " " +  keyword1, dag=dag)

# Cleaning the ranking to put it together with all the historical ranks in master file
KW1_t2 = BashOperator(task_id="CleaningRank1", bash_command="python ~/dags/kw1/scripts/clean_rank.py" , dag=dag)

#Moving file to another folder so that when we run it tomorrow there is no folder there and we can append the new rank to the master file
KW1_t3 = BashOperator(task_id="MovingCSVtofolder1", bash_command="mv ~/dags/kw1/rankings/*.csv ~/dags/kw1/rankings/keywords" , dag=dag)




#Keyword 2 #

KW2_t1 = BashOperator(task_id="Sleep20", bash_command="sleep 20", dag=dag)

KW2_t2 = BashOperator(task_id="Gettingrankings2", bash_command="python ~/dags/kw2/scripts/rank.py" + " " + url + " " +  keyword2, dag=dag)

KW2_t3 = BashOperator(task_id="CleaningRanks2", bash_command="python ~/dags/kw2/scripts/clean_rank.py" , dag=dag)

KW2_t4 = BashOperator(task_id="MovingCSVtofolder2", bash_command="mv ~/dags/kw2/rankings/*.csv ~/dags/kw2/rankings/keywords" , dag=dag)



#Keyword 3 #

KW3_t1 = BashOperator(task_id="Sleep30", bash_command="sleep 30",dag=dag)

KW3_t2 = BashOperator(task_id="Gettingrankings3", bash_command="python ~/dags/kw3/scripts/rank.py" + " " + url + " " +  keyword3, dag=dag)

KW3_t3 = BashOperator(task_id="CleaningRanks3", bash_command="python ~/dags/kw3/scripts/clean_rank.py" , dag=dag)

KW3_t4 = BashOperator(task_id="MovingCSVtofolder3", bash_command="mv ~/dags/kw3/rankings/*.csv ~/dags/kw3/rankings/keywords" , dag=dag)


#Keyword 4 #

KW4_t1 = BashOperator(task_id="Sleep42", bash_command="sleep 42",dag=dag)

KW4_t2 = BashOperator(task_id="Gettingrankings4", bash_command="python ~/dags/kw4/scripts/rank.py" + " " + url + " " + keyword4, dag=dag)

KW4_t3 = BashOperator(task_id="CleaningRanks4", bash_command="python ~/dags/kw4/scripts/clean_rank.py" , dag=dag)

KW4_t4 = BashOperator(task_id="MovingCSVtofolder4", bash_command="mv ~/dags/kw4/rankings/*.csv ~/dags/kw4/rankings/keywords" , dag=dag)



#Keyword 5 #

KW5_t1 = BashOperator(task_id="Sleep50", bash_command="sleep 50",dag=dag)

KW5_t2 = BashOperator(task_id="Gettingrankings5", bash_command="python ~/dags/kw5/scripts/rank.py" + " " + url + " " +  keyword5 , dag=dag)

KW5_t3 = BashOperator(task_id="CleaningRanks5", bash_command="python ~/dags/kw5/scripts/clean_rank.py" , dag=dag)

KW5_t4 = BashOperator(task_id="MovingCSVtofolder5", bash_command="mv ~/dags/kw5/rankings/*.csv ~/dags/kw5/rankings/keywords" , dag=dag)



# #Keyword 6 #

KW6_t1 = BashOperator(task_id="Sleep10", bash_command="sleep 10",dag=dag)

KW6_t2 = BashOperator(task_id="Gettingrankings6", bash_command="python ~/dags/kw6/scripts/rank.py" + " " + url + " " +  keyword6, dag=dag)

KW6_t3 = BashOperator(task_id="CleaningRanks6", bash_command="python ~/dags/kw6/scripts/clean_rank.py" , dag=dag)

KW6_t4 = BashOperator(task_id="MovingCSVtofolder6", bash_command="mv ~/dags/kw6/rankings/*.csv ~/dags/kw6/rankings/keywords" , dag=dag)


#Keyword 7 #

KW7_t1 = BashOperator(task_id="Sleep20", bash_command="sleep 20",dag=dag)

KW7_t2 = BashOperator(task_id="Gettingrankings7", bash_command="python ~/dags/kw7/scripts/rank.py" + " " + url + " " +  keyword7, dag=dag)

KW7_t3 = BashOperator(task_id="CleaningRanks7", bash_command="python ~/dags/kw7/scripts/clean_rank.py" , dag=dag)

KW7_t4 = BashOperator(task_id="MovingCSVtofolder7", bash_command="mv ~/dags/kw7/rankings/*.csv ~/dags/kw7/rankings/keywords" , dag=dag)




# #Keyword 8 #

KW8_t1 = BashOperator(task_id="Sleep30", bash_command="sleep 30",dag=dag)

KW8_t2 = BashOperator(task_id="Gettingrankings8", bash_command="python ~/dags/kw8/scripts/rank.py" + " " + url + " " +  keyword8, dag=dag)

KW8_t3 = BashOperator(task_id="CleaningRanks8", bash_command="python ~/dags/kw8/scripts/clean_rank.py" , dag=dag)

KW8_t4 = BashOperator(task_id="MovingCSVtofolder8", bash_command="mv ~/dags/kw8/rankings/*.csv ~/dags/kw8/rankings/keywords" , dag=dag)




# #Keyword 9 #

KW9_t1 = BashOperator(task_id="Sleep10", bash_command="sleep 10",dag=dag)

KW9_t2 = BashOperator(task_id="Gettingrankings9", bash_command="python ~/dags/kw9/scripts/rank.py" + " " + url + " " +  keyword9, dag=dag)

KW9_t3 = BashOperator(task_id="CleaningRanks9", bash_command="python ~/dags/kw9/scripts/clean_rank.py" , dag=dag)

KW9_t4 = BashOperator(task_id="MovingCSVtofolder9", bash_command="mv ~/dags/kw9/rankings/*.csv ~/dags/kw9/rankings/keywords" , dag=dag)



# #Keyword 10 #

KW10_t1 = BashOperator(task_id="Sleep55", bash_command="sleep 55",dag=dag)

KW10_t2 = BashOperator(task_id="Gettingrankings10", bash_command="python ~/dags/kw10/scripts/rank.py" + " " + url + " " +  keyword10, dag=dag)

KW10_t3 = BashOperator(task_id="CleaningRanks10", bash_command="python ~/dags/kw10/scripts/clean_rank.py" , dag=dag)

KW10_t4 = BashOperator(task_id="MovingCSVtofolder10", bash_command="mv ~/dags/kw10/rankings/*.csv ~/dags/kw10/rankings/keywords" , dag=dag)



KW11_t1 = BashOperator(task_id="Sleep11", bash_command="sleep 25",dag=dag3)
KW11_t2 = BashOperator(task_id="Gettingrankings11", bash_command="python ~/dags/kw11/scripts/rank.py" + " " + url + " " +  keyword11, dag=dag3)

KW11_t3 = BashOperator(task_id="CleaningRanks11", bash_command="python ~/dags/kw11/scripts/clean_rank.py" , dag=dag3)

KW11_t4 = BashOperator(task_id="MovingCSVtofolder11", bash_command="mv ~/dags/kw11/rankings/*.csv ~/dags/kw11/rankings/keywords" , dag=dag3)




sub_dag = SubDagOperator(subdag=sub_dag(PARENT_DAG_NAME, CHILD_DAG_NAME, dag.start_date, dag.schedule_interval),
  task_id=CHILD_DAG_NAME,
  dag=dag,
)

# Process (Dont change order)

KW1_t1 >> KW1_t2 >> KW1_t3  >> sub_dag

KW2_t1 >> KW2_t2 >> KW2_t3 >> KW2_t4  >> sub_dag

KW3_t1 >> KW3_t2 >> KW3_t3 >> KW3_t4  >> sub_dag

KW4_t1 >> KW4_t2 >> KW4_t3 >> KW4_t4  >> sub_dag

KW5_t1 >> KW5_t2 >> KW5_t3 >> KW5_t4  >> sub_dag

KW6_t1 >> KW6_t2 >> KW6_t3 >> KW6_t4  >> sub_dag

KW7_t1 >> KW7_t2 >> KW7_t3 >> KW7_t4  >> sub_dag

KW8_t1 >> KW8_t2 >> KW8_t3 >> KW8_t4  >> sub_dag

KW9_t1 >> KW9_t2 >> KW9_t3 >> KW9_t4  >> sub_dag

KW10_t1 >> KW10_t2 >> KW10_t3 >> KW10_t4 >> sub_dag

KW11_t1 >> KW11_t2 >> KW11_t3 >> KW11_t4 >> sub_dag


dag3 = DAG("Keywords_check_2", default_args=default_args, schedule_interval='5 10 * * *')

# #Keyword 11 #


# # #Keyword 12 #

# KW12_t1 = BashOperator(task_id="Sleep12", bash_command="sleep 35",dag=dag3)

# KW12_t2 = BashOperator(task_id="Gettingrankings12", bash_command="python ~/dags/kw12/scripts/rank.py" + " " + url + " " +  keyword12, dag=dag3)

# KW12_t3 = BashOperator(task_id="CleaningRanks12", bash_command="python ~/dags/kw12/scripts/clean_rank.py" , dag=dag3)

# KW12_t4 = BashOperator(task_id="MovingCSVtofolder12", bash_command="mv ~/dags/kw12/rankings/*.csv ~/dags/kw12/rankings/keywords" , dag=dag3)

# KW12_t5 = PythonOperator(task_id='upload_to_S3_12', python_callable=upload_file_to_S3_with_hook, 
#         op_kwargs={
#         'filename': '/usr/local/airflow/dags/kw12/kw12.csv',
#         'key': 'kw12.csv',
#         'bucket_name': 'bucket-test85',
#         'replace':'True'
#         },
#         dag=dag3)

# # #Keyword 13 #

# # KW13_t1 = BashOperator(task_id="Sleep13", bash_command="sleep 25",dag=dag3)

# # KW13_t2 = BashOperator(task_id="Gettingrankings13", bash_command="python ~/dags/kw13/scripts/rank.py" + " " + url + " " +  keyword13, dag=dag3)

# # KW13_t3 = BashOperator(task_id="CleaningRanks13", bash_command="python ~/dags/kw13/scripts/clean_rank.py" , dag=dag3)

# # KW13_t4 = BashOperator(task_id="MovingCSVtofolder13", bash_command="mv ~/dags/kw13/rankings/*.csv ~/dags/kw13/rankings/keywords" , dag=dag3)


# # # #Keyword 14 #

# # KW14_t1 = BashOperator(task_id="Sleep14", bash_command="sleep 25",dag=dag3)

# # KW14_t2 = BashOperator(task_id="Gettingrankings14", bash_command="python ~/dags/kw14/scripts/rank.py" + " " + url + " " +  keyword14, dag=dag3)

# # KW14_t3 = BashOperator(task_id="CleaningRanks14", bash_command="python ~/dags/kw14/scripts/clean_rank.py" , dag=dag3)

# # KW14_t4 = BashOperator(task_id="MovingCSVtofolder14", bash_command="mv ~/dags/kw14/rankings/*.csv ~/dags/kw14/rankings/keywords" , dag=dag3)


# # # #Keyword 15 #

# # KW15_t1 = BashOperator(task_id="Sleep15", bash_command="sleep 25",dag=dag3)

# # KW15_t2 = BashOperator(task_id="Gettingrankings15", bash_command="python ~/dags/kw15/scripts/rank.py" + " " + url + " " +  keyword15, dag=dag3)

# # KW15_t3 = BashOperator(task_id="CleaningRanks15", bash_command="python ~/dags/kw15/scripts/clean_rank.py" , dag=dag3)

# # KW15_t4 = BashOperator(task_id="MovingCSVtofolder15", bash_command="mv ~/dags/kw15/rankings/*.csv ~/dags/kw15/rankings/keywords" , dag=dag3)






# KW12_t1 >> KW12_t2 >> KW12_t3 >> KW12_t4 >> KW12_t5 

# # KW13_t1 >> KW13_t2 >> KW13_t3 >> KW13_t4

# # KW14_t1 >> KW14_t2 >> KW14_t3 >> KW14_t4

# # KW15_t1 >> KW15_t2 >> KW15_t3 >> KW15_t4


