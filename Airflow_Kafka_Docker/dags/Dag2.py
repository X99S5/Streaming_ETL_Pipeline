from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime,timedelta
from time import sleep


default_args = {
    'owner' : 'me9',
    'retries' : 0,
    'retry_delay': timedelta(minutes = 2)

}

with DAG(
    dag_id = '2',
    default_args = default_args,
    description = 'DAG #2',
    start_date = datetime(2023,10,23, 11, 13, 0),
    schedule_interval = '@daily',
    catchup = False
    
) as dag:
    task1 = BashOperator(
        task_id = 'Consumer1',
        bash_command= 'spark-submit /opt/SpdTask.py'
    )

    task1