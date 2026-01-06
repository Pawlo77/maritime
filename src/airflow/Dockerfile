FROM apache/airflow:3.1.3-python3.10

USER root

RUN apt update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get install -y ant && \
    apt-get clean;

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
