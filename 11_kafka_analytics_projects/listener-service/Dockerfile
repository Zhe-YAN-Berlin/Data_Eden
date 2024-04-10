FROM ubuntu

WORKDIR /app

RUN apt-get update && \
    apt-get install -y python3-pip curl &&  \
    pip3 install confluent-kafka

CMD ["sleep", "99999"]