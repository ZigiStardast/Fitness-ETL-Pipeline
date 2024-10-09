FROM apache/airflow:2.9.3-python3.9
USER root
RUN apt-get update -qq && apt-get install vim -qqq

RUN apt-get install default-jdk -y
ENV JAVA_HOME /usr/lib/jvm/default-java/
RUN export JAVA_HOME
ENV PATH="${JAVA_HOME}/bin:${PATH}"
RUN export PATH

USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt