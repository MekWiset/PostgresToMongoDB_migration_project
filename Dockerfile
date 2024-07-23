FROM apache/airflow:2.9.1-python3.9

WORKDIR /opt/airflow

COPY requirements.txt .

USER root
RUN apt-get update

USER airflow
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8080