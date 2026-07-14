FROM apache/airflow:2.9.1-python3.12

USER root
RUN apt-get update && apt-get install -y openssh-client && apt-get clean


USER airflow

# Downloading SSHOperator
RUN pip install --no-cache-dir apache-airflow-providers-ssh