FROM ubuntu

WORKDIR /app

RUN apt-get update && \
    apt-get install -y python3-pip curl && \
    pip3 install confluent-kafka
    
#added
COPY kafka_events.py .
COPY kafka_topics.py .
COPY launch.sh .
COPY texts /app/texts

CMD ["bash", "launch.sh"]

