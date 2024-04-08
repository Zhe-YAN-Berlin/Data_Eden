sudo docker build . -t kafka_events_image
sudo docker image ls
sudo docker run -p 8888:8888 --name kafka_events_docker -d 4d983c5d24f8

sudo docker ps -a

sudo docker logs 7f2ee5c7dcdf
sudo docker stop 1dcc49355ef2 e29eec979c79 aeddc49d34cc a0946a1f8a53 a971c2ee6540
sudo docker rm 1dcc49355ef2 e29eec979c79 aeddc49d34cc a0946a1f8a53 a971c2ee6540

sudo docker rmi 7da6e0b79600 8798600b7aae a379ed56291a 21aa4d4e6ed0

docker-compose up

a container named listener-service will be spun up solely to allow exploration of the data stream. To access it, simply enter the container with the following command: 
docker-compose exec listener-service bash
and start the following process: 
python3 kafka_consumer.py.