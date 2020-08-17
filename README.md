# Kafka Implementation using Python

#### Start the server
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

#### Topics Covered
* Producer
    * Simple Producer
    * With Callback
    * With Key
* Consumer
    * Simple Consumer
    * With Threads
    * Assign and Seek
 
 
#### Notes
* Kafka clients and brokers have bidirectional compatibility
i.e. older client (say 1.1) can talk to newer broker (say 2.0) and vice versa
