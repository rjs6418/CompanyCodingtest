from kafka import KafkaProducer
import json

from datetime import datetime
import uuid
from random import *
import time

DATATOPIC = "DATATOPIC"
BROKERS = ["localhost:9091", "localhost:9092", "localhost:9093"]

producer = KafkaProducer(bootstrap_servers=BROKERS)

while True:
    data = (str(datetime.now()), str(datetime.now()), str(uuid.uuid4()), uniform(0.200000, 0.300000), randint(1, 1000), choice(["BUY", "SELL"]))
    producer.send(DATATOPIC, json.dumps(data).encode("utf-8"))
    print(data)
    time.sleep(0.1)

    
    



