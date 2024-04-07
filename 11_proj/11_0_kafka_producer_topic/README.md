sudo docker build . -t kafka_events_image
sudo docker image ls
sudo docker run -p 9092:9092 --name kafka_events_docker -d 21aa4d4e6ed0

sudo docker ps -a

sudo docker logs 7f2ee5c7dcdf
sudo docker stop 7f2ee5c7dcdf
sudo docker rm 7f2ee5c7dcdf

sudo docker rmi 11f01809c857