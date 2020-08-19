from kafka import KafkaProducer
import json
from data import get_registered_user
import time


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=json_serializer)

if __name__ == "__main__":
    count = 30
    while count > 0:
        count -= 1
        producer.send('myfirst_topic', get_registered_user())
        time.sleep(2)
