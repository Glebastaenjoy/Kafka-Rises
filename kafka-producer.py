from kafka import KafkaProducer
import json
from data import get_registered_user
import time


def json_serializer(data):
    """ Serialize the data sending via network"""
    return json.dumps(data).encode('utf-8')


def get_partition(key_bytes, all_partition, available_partition):
    """ Select the partition in which to send the data """
    return available_partition[0]


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=json_serializer,
                         # partitioner=get_partition
                         )

if __name__ == "__main__":
    count = 30
    while count > 0:
        count -= 1
        producer.send('myfirst_topic', get_registered_user())
        time.sleep(1)
