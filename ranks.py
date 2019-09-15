from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os
from keywords import *  

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



dag = DAG("Keywords_check1", default_args=default_args, schedule_interval='10 10 * * *')

# Keyword 1 #

KW1_t1 = BashOperator(task_id="Gettingrankings1", bash_command="python ~/dags/kw1/scripts/rank.py" + url +  keyword1, dag=dag)

KW1_t2 = BashOperator(task_id="CleaningRank1", bash_command="python ~/dags/kw1/scripts/clean_rank.py" , dag=dag)

KW1_t3 = BashOperator(task_id="MovingCSVtofolder1", bash_command="mv ~/dags/kw1/rankings/*.csv ~/dags/kw1/rankings/keywords" , dag=dag)

KW1_t4 = BashOperator(task_id="Pushing_to_db1", bash_command="sleep 10",dag=dag)

#Keyword 2 #

KW2_t1 = BashOperator(task_id="Sleep2", bash_command="sleep 20", dag=dag)

KW2_t2 = BashOperator(task_id="Gettingrankings2", bash_command="python ~/dags/kw2/scripts/rank.py" + url +  keyword2, dag=dag)

KW2_t3 = BashOperator(task_id="CleaningRanks2", bash_command="python ~/dags/kw2/scripts/clean_rank.py" , dag=dag)

KW2_t4 = BashOperator(task_id="MovingCSVtofolder2", bash_command="mv ~/dags/kw2/rankings/*.csv ~/dags/kw2/rankings/keywords" , dag=dag)


#Keyword 3 #

KW3_t1 = BashOperator(task_id="Sleep3", bash_command="sleep 30",dag=dag)

KW3_t2 = BashOperator(task_id="Gettingrankings3", bash_command="python ~/dags/kw3/scripts/rank.py" + url +  keyword3, dag=dag)

KW3_t3 = BashOperator(task_id="CleaningRanks3", bash_command="python ~/dags/kw3/scripts/clean_rank.py" , dag=dag)

KW3_t4 = BashOperator(task_id="MovingCSVtofolder3", bash_command="mv ~/dags/kw3/rankings/*.csv ~/dags/kw3/rankings/keywords" , dag=dag)

#Keyword 4 #

KW4_t1 = BashOperator(task_id="Sleep4", bash_command="sleep 42",dag=dag)

KW4_t2 = BashOperator(task_id="Gettingrankings4", bash_command="python ~/dags/kw4/scripts/rank.py" + url +  keyword4, dag=dag)

KW4_t3 = BashOperator(task_id="CleaningRanks4", bash_command="python ~/dags/kw4/scripts/clean_rank.py" , dag=dag)

KW4_t4 = BashOperator(task_id="MovingCSVtofolder4", bash_command="mv ~/dags/kw4/rankings/*.csv ~/dags/kw4/rankings/keywords" , dag=dag)


#Keyword 5 #

KW5_t1 = BashOperator(task_id="Sleep5", bash_command="sleep 50",dag=dag)

KW5_t2 = BashOperator(task_id="Gettingrankings5", bash_command="python ~/dags/kw5/scripts/rank.py" + url +  keyword5 , dag=dag)

KW5_t3 = BashOperator(task_id="CleaningRanks5", bash_command="python ~/dags/kw5/scripts/clean_rank.py" , dag=dag)

KW5_t4 = BashOperator(task_id="MovingCSVtofolder5", bash_command="mv ~/dags/kw5/rankings/*.csv ~/dags/kw5/rankings/keywords" , dag=dag)


# Process (Dont change order)

KW1_t1 >> KW1_t2 >> KW1_t3 >> KW1_t4

KW2_t1 >> KW2_t2 >> KW2_t3 >> KW2_t4

KW3_t1 >> KW3_t2 >> KW3_t3 >> KW3_t4

KW4_t1 >> KW4_t2 >> KW4_t3 >> KW4_t4

KW5_t1 >> KW5_t2 >> KW5_t3 >> KW5_t4



dag2 = DAG("Keywords_check2", default_args=default_args, schedule_interval='10 15 * * *')


# #Keyword 6 #

KW6_t1 = BashOperator(task_id="Sleep6", bash_command="sleep 10",dag=dag2)

KW6_t2 = BashOperator(task_id="Gettingrankings6", bash_command="python ~/dags/kw6/scripts/rank.py" + url +  keyword6, dag=dag2)

KW6_t3 = BashOperator(task_id="CleaningRanks6", bash_command="python ~/dags/kw6/scripts/clean_rank.py" , dag=dag2)

KW6_t4 = BashOperator(task_id="MovingCSVtofolder6", bash_command="mv ~/dags/kw6/rankings/*.csv ~/dags/kw6/rankings/keywords" , dag=dag2)

#Keyword 7 #

KW7_t1 = BashOperator(task_id="Sleep7", bash_command="sleep 20",dag=dag2)

KW7_t2 = BashOperator(task_id="Gettingrankings7", bash_command="python ~/dags/kw7/scripts/rank.py" + url +  keyword7, dag=dag2)

KW7_t3 = BashOperator(task_id="CleaningRanks7", bash_command="python ~/dags/kw7/scripts/clean_rank.py" , dag=dag2)

KW7_t4 = BashOperator(task_id="MovingCSVtofolder7", bash_command="mv ~/dags/kw7/rankings/*.csv ~/dags/kw7/rankings/keywords" , dag=dag2)


# #Keyword 8 #

KW8_t1 = BashOperator(task_id="Sleep8", bash_command="sleep 30",dag=dag2)

KW8_t2 = BashOperator(task_id="Gettingrankings8", bash_command="python ~/dags/kw8/scripts/rank.py" + url +  keyword8, dag=dag2)

KW8_t3 = BashOperator(task_id="CleaningRanks8", bash_command="python ~/dags/kw8/scripts/clean_rank.py" , dag=dag2)

KW8_t4 = BashOperator(task_id="MovingCSVtofolder8", bash_command="mv ~/dags/kw8/rankings/*.csv ~/dags/kw8/rankings/keywords" , dag=dag2)



# #Keyword 9 #

KW9_t1 = BashOperator(task_id="Sleep9", bash_command="sleep 10",dag=dag2)

KW9_t2 = BashOperator(task_id="Gettingrankings9", bash_command="python ~/dags/kw9/scripts/rank.py" + url +  keyword9, dag=dag2)

KW9_t3 = BashOperator(task_id="CleaningRanks9", bash_command="python ~/dags/kw9/scripts/clean_rank.py" , dag=dag2)

KW9_t4 = BashOperator(task_id="MovingCSVtofolder9", bash_command="mv ~/dags/kw9/rankings/*.csv ~/dags/kw9/rankings/keywords" , dag=dag2)

# #Keyword 10 #

KW10_t1 = BashOperator(task_id="Sleep10", bash_command="sleep 55",dag=dag2)

KW10_t2 = BashOperator(task_id="Gettingrankings10", bash_command="python ~/dags/kw10/scripts/rank.py" + url +  keyword10, dag=dag2)

KW10_t3 = BashOperator(task_id="CleaningRanks10", bash_command="python ~/dags/kw10/scripts/clean_rank.py" , dag=dag2)

KW10_t4 = BashOperator(task_id="MovingCSVtofolder10", bash_command="mv ~/dags/kw10/rankings/*.csv ~/dags/kw10/rankings/keywords" , dag=dag2)

    
# Process (Dont change order)

KW6_t1 >> KW6_t2 >> KW6_t3 >> KW6_t4

KW7_t1 >> KW7_t2 >> KW7_t3 >> KW7_t4

KW8_t1 >> KW8_t2 >> KW8_t3 >> KW8_t4

KW9_t1 >> KW9_t2 >> KW9_t3 >> KW9_t4

KW10_t1 >> KW10_t2 >> KW10_t3 >> KW10_t4


