FROM apache/airflow:2.9.3
USER root
RUN apt-get update -qq && apt-get install vim -qqq
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt