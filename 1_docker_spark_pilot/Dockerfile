FROM datamechanics/spark:3.1.1-hadoop-3.2.0-java-8-scala-2.12-python-3.8-dm18

USER root

WORKDIR /opt/spark

# download Kafka connector JAR files to Spark jars folders
#RUN wget -O /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.1.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.2.1/spark-sql-kafka-0-10_2.12-3.1.1.jar
#RUN wget -O /opt/spark/jars/spark-streaming-kafka-0-10_2.12-3.1.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.1.3/spark-streaming-kafka-0-10_2.12-3.1.1.jar
#RUN wget -O /opt/spark/jars/kafka-clients-3.1.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.0/kafka-clients-3.1.1.jar
#RUN wget -O /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.1.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.1.3/spark-token-provider-kafka-0-10_2.12-3.1.1.jar
# https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

RUN pip install --upgrade pip

COPY  requirements.txt .
RUN pip3 install -r requirements.txt

#COPY etl_task_2.py .

CMD jupyter-lab --allow-root --no-browser --ip=0.0.0.0