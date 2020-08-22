from kafka import KafkaProducer
import json
from data import get_registered_user


def json_serializer(data):
    """ Serialize the data sending via network"""
    return json.dumps(data).encode('utf-8')


def get_partition(key_bytes, all_partition, available_partition):
    """ Select the partition in which to send the data """
    return available_partition[0]


def kafka_python_producer_sync(producer, topic, size):
    for n in range(size):
        data = get_registered_user()
        data['serial'] = n
        future = producer.send('myfirst_topic', data)
        result = future.get(timeout=60)
        print(result)
    producer.flush()


def success(metadata):
    print(metadata)


def error(exception):
    print(exception)


def kafka_python_producer_async(producer, topic, size):
    for n in range(size):
        data = get_registered_user()
        data['serial'] = n
        producer.send(topic, data).add_callback(success).add_errback(error)
    producer.flush()


if __name__ == "__main__":
    topic = 'myfirst_topic'
    server_list = ['localhost:9092']
    producer = KafkaProducer(bootstrap_servers=server_list,
                             value_serializer=json_serializer,
                             # partitioner=get_partition
                             )

    # kafka_python_producer_async(producer, topic, 10)
    kafka_python_producer_sync(producer, topic, 10)
