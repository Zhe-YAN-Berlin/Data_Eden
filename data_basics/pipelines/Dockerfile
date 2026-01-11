--docker file for Apache Airflow 2.X with Python 3.10

FROM apache/airflow:2.9.3-python3.10

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

copy ./requirements.txt /tmp

RUN pip install --no-cache-dir --updagrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt



    

