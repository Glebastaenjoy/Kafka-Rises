from kafka import KafkaConsumer
import json

if __name__ == "__main__":
    consumer = KafkaConsumer(
        'myfirst_topic',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        group_id='console-consumer-94126'
    )
    for msg in consumer:
        print(msg.value)
