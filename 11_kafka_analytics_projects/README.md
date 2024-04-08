sudo docker build . -t kafka_events_image
sudo docker image ls
sudo docker run -p 9092:9092 --name kafka_events_docker -d 21aa4d4e6ed0

sudo docker ps -a

sudo docker logs 7f2ee5c7dcdf
sudo docker stop 1dcc49355ef2 e29eec979c79 aeddc49d34cc a0946a1f8a53 a971c2ee6540
sudo docker rm 1dcc49355ef2 e29eec979c79 aeddc49d34cc a0946a1f8a53 a971c2ee6540

sudo docker rmi 7da6e0b79600 8798600b7aae a379ed56291a 21aa4d4e6ed0

docker-compose up