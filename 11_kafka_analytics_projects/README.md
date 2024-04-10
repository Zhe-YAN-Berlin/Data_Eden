docker network create kafka-spark-network
docker network ls

sudo docker build . -t spark4kafka_image
sudo docker image ls
# this docker only built by myself PLUS mounted files but dockerOperator will NOT leverage this docker, but create another one
sudo docker run -p 8888:8888 --network 11_kafka_analytics_projects_default --name spark4kafka -d 7c8738eb1cc5 sleep infinity
sudo docker ps
sudo docker inspect 8e9bb82ef0ac

sudo docker images
sudo docker stop c69432811c9a 748682c87c24 2a0fa24c73a1 fbb7a2c0574e 2cd11c89a8b4

sudo docker rm 0dfe943d8a60 6e320dc8cfa5 8254ba64cc2e 597332e98bb8 60afec1b40b4 03e2c2fdfe98
sudo docker rmi f1cf46142655 63fddf0987d4 c1076c3d31f9 c321b884b8db
sudo docker network rm f1cf46142655 63fddf0987d4 c1076c3d31f9 c321b884b8db

# enter docker bash
sudo docker exec -it ad62e10d78d5 /bin/bash
python3 kafka_consumer.py
# check logs 
sudo docker logs 0dfe943d8a60
# get jupyter lab  !!
jupyter lab --ip='0.0.0.0' --port=8888 --no-browser --allow-root

python3 kafka_consumer.py

cd /home/sparkuser/.local/share/jupyter/runtime
# exit 
CTRL+D
# try to get file path and files displayed
cd /temp

# TEST api
curl -X 'POST' \
  'http://classification-service:5000/predict' \
  -H 'Content-Type: application/json' \
  -d '{
  "text": "Frodo felt a  strange certainty that in this matter Gollum was for once  not so far from the truth as might be suspected.But even  if Gollum  could  be trusted on this  point, Frodo did not forget the wiles  of the  Enemy"
}'

# check docker sock program 
sudo service docker status

# kill airflow 
pkill -9 -f "airflow scheduler" 
pkill -9 -f "airflow webserver" 
pkill -9 -f "gunicorn"


https://hub.docker.com/r/datamechanics/spark
https://developershome.blog/2023/01/29/deploy-spark-using-docker/
https://www.youtube.com/watch?v=WSfVEOsLTD8


a container named listener-service will be spun up solely to allow exploration of the data stream. To access it, simply enter the container with the following command: 
docker-compose exec listener-service bash
and start the following process: 
python3 kafka_consumer.py.

which java
java --version
spark-shell
pyspark --version
which pyspark

echo 'export JAVA_HOME="${HOME}/usr/local/openjdk-11"' >> ~/.bashrc
echo 'export PATH="${JAVA_HOME}/bin:${PATH}"' >> ~/.bashrc
echo 'export SPARK_HOME="${HOME}/spark' >> ~/.bashrc
echo 'export PATH="${SPARK_HOME}/bin:${PATH}"' >> ~/.bashrc

export JAVA_HOME="${HOME}/usr/local/openjdk-8/"
export PATH="${JAVA_HOME}/bin:${PATH}"
export SPARK_HOME="${HOME}/opt/spark/spark-3.4.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
source .bashrc

sudo apt-get update
sudo apt-get install nano

