FROM apache/airflow:2.11.1-python3.10

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark>=4.11.0 \
    apache-airflow-providers-amazon>=9.5.0 \
    pyspark==3.5.4
