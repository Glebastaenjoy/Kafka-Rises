from kafka import KafkaConsumer, TopicPartition
import json

if __name__ == "__main__":
    server_list = ['localhost:9092']
    consumer = KafkaConsumer(bootstrap_servers= server_list)
    # consumer.subscribe(['myfirst_topic'])
    consumer.assign([TopicPartition('myfirst_topic', 0), TopicPartition('myfirst_topic', 1)])
    for msg in consumer:
        print(msg)
