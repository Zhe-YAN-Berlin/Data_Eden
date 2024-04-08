docker network create kafka-spark-network
docker network ls

sudo docker build . -t spark4kafka_image
sudo docker image ls
# this docker only built by myself PLUS mounted files but dockerOperator will NOT leverage this docker, but create another one
sudo docker run -p 8888:8888 --network 9c75b7d816cf --name spark4kafka -d 4d983c5d24f8 sleep infinity
sudo docker ps
sudo docker inspect 8e9bb82ef0ac


sudo docker images
sudo docker ps
sudo docker stop c69432811c9a 748682c87c24 2a0fa24c73a1 fbb7a2c0574e 2cd11c89a8b4

sudo docker rm d783b267e57b b9ee2567e17e d5e88d3b3bfe f1b084a50013 db1cfe8a14d7 1f0de13d9b49
sudo docker rmi ae245de64565 dfe67803e114 eb65f8f959a6
sudo docker network rm 4022ff7fbdf2 fe650a0b0087

# enter docker bash
sudo docker exec -it 6302070937b3 /bin/bash
# try to get file path and files displayed
cd /temp

# TEST api
curl -X 'POST' \
  'http://classification-service:5000/predict' \
  -H 'Content-Type: application/json' \
  -d '{
  "text": " Albus Dumbledore didnt seem to realize that he had just arrived in a street where everything from his name to his boots was unwelcome. He was busy rummaging in his cloak, looking for something. But he did seem to realize he was being watched, because he looked up suddenly at the cat, which was still staring at him from the other end of the street."
}'

# check logs 
sudo docker logs 76a371e2dcdb
# get jupyter lab  !!
jupyter lab --ip='0.0.0.0' --port=8888 --no-browser --allow-root

# check docker sock program 
sudo service docker status

# kill airflow 
pkill -9 -f "airflow scheduler" 
pkill -9 -f "airflow webserver" 
pkill -9 -f "gunicorn"


https://hub.docker.com/r/datamechanics/spark
https://developershome.blog/2023/01/29/deploy-spark-using-docker/
https://www.youtube.com/watch?v=WSfVEOsLTD8
