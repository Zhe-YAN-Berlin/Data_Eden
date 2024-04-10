import os
import json
import time
import string
import random
from confluent_kafka import Producer
import requests

random.seed(10)
DATA_FOLDER = 'texts'

def read_file(path):
    with open(path, 'r') as f:
        data = json.load(f)
    return data

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def get_label(text):
    url = 'http://classification-service:5000/predict'
    headers = {'Content-Type': 'application/json'}
    data = {
        "text": text
    }
    response = requests.post(url, headers=headers, json=data)
    return response.json()['label']

def gen_message(files):
    f = random.choice(files)
    try:
        content = read_file(f'{DATA_FOLDER}/{f}')
        msg = {
            "id": content["id"],
            "time": int(time.time()),
            "readers": random.randint(1e2, 1e5),
            "text": content["text"],
            "label": get_label(content["text"])
        }
        
    except UnicodeDecodeError:
        print("Read error", f)
        msg = {
            "id": "000000",
            "time": int(time.time()),
            "readers": random.randint(1e2, 1e5),
            "text": "",
            "label": ""
        }
    return msg

p = Producer({'bootstrap.servers': 'kafka'})
files = [f for f in os.listdir(DATA_FOLDER) if not '_' in f]

while True:
    msg = gen_message(files)
    p.poll(0)
    p.produce('events', json.dumps(msg).encode('utf-8'), callback=delivery_report)
    time.sleep(random.random()*2)

p.flush()
