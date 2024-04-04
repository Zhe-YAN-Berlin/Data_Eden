https://hub.docker.com/r/datamechanics/spark

https://developershome.blog/2023/01/29/deploy-spark-using-docker/

https://www.youtube.com/watch?v=WSfVEOsLTD8


sudo docker build . -t sparkhome
sudo docker image ls
# this docker only built by myself PLUS mounted files but dockerOperator will NOT leverage this docker, but create another one
sudo docker run -p 8888:8888 --name spark -d --volume /home/datatalks_jan/Data_Eden/8_pySpark_pilot/source_data/multi_source_data:/opt/spark/mount_data 129bb0debc9e sleep infinity
# mount successfully!

sudo docker images
sudo docker ps
sudo docker stop 7828098e8778
sudo docker rm 7828098e8778
sudo docker rmi 629b48d46e08

# enter docker bash
sudo docker exec -it 5c2ee358d290 /bin/bash
# try to get file path and files displayed
cd /temp

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
