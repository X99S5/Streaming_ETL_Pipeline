# Streaming ETL Pipeline

This project involves building an ETL pipeline which is orchestrated with airflow and run via docker containers.

Data is generated and sent to a Kafka cluster via kafka-python . The data is then extracted by pyspark which runs a summing aggregation on the data and outputs to console.

## Setup
#1) Build the main image<br> 
     -> docker build . --tag extending_airflow:latest<br>

#2) Startup zookeeper and wait ~5 seconds<br> 
     -> docker compose up -d --remove-orphans zookeeper<br>

#3) Startup the airflow and kafka containers<br> 
     -> docker compose up -d --remove-orphans<br>

#4) In any kafka container bash shell , run the command to create a topic with 10 partitions<br> 
     -> opt/kafka_2.13-2.8.1/bin/kafka-topics.sh --create --topic topic_a --bootstrap-server kafka1:29092 --partitions 10 --replication-factor 1<br>

#5) Wait for airflow to startup , then copy the pyspark code to the airflow schedular and triggerer for it to run<br> 
     -> docker cp ./SpdTask.py airflow_kafka_docker-airflow-triggerer-1:/opt<br>
     -> docker cp ./SpdTask.py airflow_kafka_docker-airflow-scheduler-1:/opt<br>
    
#6) Log into airflow ui on localhost port 8080 and start Dag 2 which runs the pyspark code

#7) Start Dag1 which produces the messages.

#8) Observe logs of Dag2 for output.
