# Kafka Implementation using Python

## Core Revision

#### Start the server
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

#### Topics
* kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic myfirst_topic --create --partitions 3 --replication-factor 1
* kafka-topics.sh --zookeeper localhost:2181 --list
* kafka-topics.sh --zookeeper localhost:2181 --topic myfirst_topic --describe
* kafka-topics.sh --zookeeper localhost:2181 --topic mysecond_topic --delete
Note: This will have no impact if delete.topic.enable is not set to true.

#### Console Producers
* kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic myfirst_topic
* kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic myfirst_topic --producer-property acks=all
* kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic brand_new_topic

#### Console Consumers
* kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic myfirst_topic
* kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic myfirst_topic --from-beginning
* kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic myfirst_topic --group my_firstconsumer_group
* kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
* kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my_firstconsumer_group
* kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my_firstconsumer_group --reset-offsets --to-earliest --topic myfirst_topic --dry-run

#### Notes
* Kafka clients and brokers have bidirectional compatibility
i.e. older client (say 1.1) can talk to newer broker (say 2.0) and vice versa


## Enter Python

#### Dependencies

* pip install kafka-python
* pip install faker