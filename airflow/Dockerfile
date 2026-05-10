FROM apache/airflow:2.9.3-python3.12

USER root

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER airflow

WORKDIR /opt/airflow

COPY airflow/dags /opt/airflow/dags
