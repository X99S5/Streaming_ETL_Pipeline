from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
# docker compose up -d --no-deps --build airflow-webserver airflow-scheduler airflow-triggerer (to rebuild image after adding additional packages)
from kafka import KafkaProducer
from json import dumps
from time import sleep

def topic_message():
    topic_name = 'topic_a'
    producer = KafkaProducer( bootstrap_servers=['kafka1:29092'], value_serializer=lambda x:dumps(x).encode('utf-8'))
    
    for i in range(100):
        data = {'Packet': i}
        producer.send( topic_name, value = data)
        sleep(3)
        
default_args = {
    'owner' : 'me9',
    'retries' : 5,
    'retry_delay': timedelta(minutes = 2)

}

with DAG(
    dag_id = '1',
    default_args = default_args,
    description = 'DAG #1',
    start_date = datetime(2023,10,23, 11, 13, 0),
    schedule_interval = '@daily'
    
) as dag:
    task1 = PythonOperator(
        task_id = 'topic_message',
        python_callable=topic_message
    )

    task1