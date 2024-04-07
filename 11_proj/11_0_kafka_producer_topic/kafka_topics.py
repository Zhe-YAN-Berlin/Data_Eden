from confluent_kafka.admin import AdminClient, NewTopic

a = AdminClient({'bootstrap.servers': 'kafka'})

new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in ["events"]]

fs = a.create_topics(new_topics)

for topic, f in fs.items():
    try:
        f.result()
        print("Topic {} created".format(topic))
    except Exception as e:
        print("Failed to create topic {}: {}".format(topic, e))