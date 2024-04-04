https://hub.docker.com/r/datamechanics/spark

https://developershome.blog/2023/01/29/deploy-spark-using-docker/

https://www.youtube.com/watch?v=WSfVEOsLTD8


sudo docker build . -t sparkhome
sudo docker image ls

sudo docker run -p 8888:8888 --name spark -d --volume /home/datatalks_jan/Data_Eden/8_pySpark_pilot/source_data/multi_source_data:/opt/spark/mount_data cb004f06511d sleep infinity
# mount successfully!

sudo docker images
sudo docker ps
sudo docker stop 89ff12f39446
sudo docker rm 76a371e2dcdb
sudo docker rmi 7aa24d839e9d

# enter docker bash
sudo docker exec -it 89ff12f39446 /bin/bash
# try to get file path and files displayed
cd /temp

# check logs 
sudo docker logs 76a371e2dcdb
# get jupyter lab  !!
jupyter lab --ip='0.0.0.0' --port=8888 --no-browser --allow-root


