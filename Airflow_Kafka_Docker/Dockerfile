FROM apache/airflow:2.7.1

# Install wget and other necessary tools
USER root
RUN apt-get update && apt-get install -y wget
RUN apt-get update && apt-get install -y procps

# Define the JDK version and download URL
ENV JDK_VERSION 8
ENV JDK_URL https://download.java.net/openjdk/jdk8u41/ri/openjdk-8u41-b04-linux-x64-14_jan_2020.tar.gz

# Create a directory for JDK installation
RUN mkdir -p /opt/java

# Download and extract the JDK
RUN wget --quiet --no-check-certificate -O /opt/openjdk.tar.gz $JDK_URL && \
    tar -xzf /opt/openjdk.tar.gz -C /opt/java --strip-components=1 && \
    rm /opt/openjdk.tar.gz

# Set the JAVA_HOME environment variable
ENV JAVA_HOME /opt/java

# Update the PATH to include the JDK's bin directory
ENV PATH $JAVA_HOME/bin:$PATH







USER airflow
COPY requirements.txt /requirements.txt
COPY SpdTask.py /opt/SpdTask.py
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt


#Download jars for kafka required by pyspark (Spark v = 3.5.0 with Scala 2.12.x)
USER root
RUN curl -o /home/airflow/.local/lib/python3.8/site-packages/pyspark/jars/spark-streaming-kafka-0-10_2.12-3.5.0.jar -L 'https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.5.0/spark-streaming-kafka-0-10_2.12-3.5.0.jar'
RUN curl -o /home/airflow/.local/lib/python3.8/site-packages/pyspark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar -L "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar"
RUN curl -o /home/airflow/.local/lib/python3.8/site-packages/pyspark/jars/kafka-clients-3.5.0.jar -L "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar"
RUN curl -o /home/airflow/.local/lib/python3.8/site-packages/pyspark/jars/commons-pool2-2.12.0.jar -L 'https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar'
RUN curl -o /home/airflow/.local/lib/python3.8/site-packages/pyspark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar -L 'https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar'

USER airflow
#docker build . --tag extending_airflow:latest
#docker compose up -d --no-deps --build airflow-webserver airflow-scheduler airflow-triggerer
