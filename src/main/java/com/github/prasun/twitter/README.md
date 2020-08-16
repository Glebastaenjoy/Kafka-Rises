# Advanced Configuration

## Producer Message compression

* Compression is enabled at producers level and doesn't require any configuration changes
  in the brokers or in the consumers
* **compression.type** can be 'none'(default),'gzip','lz4','snappy'
* Smaller producer request size hence faster data transfer
* **Batching**
    * linger.ms :- number of milliseconds the producer is willing to wait before sending a batch out(default 0)
    * batch.size:- Maximum number of bytes that will be included in a batch. The default is 16kb
    * if the batch is full before the end of the linger.ms period it will be sent to kafka right away
    * with Kafka producer metrics you can monitor the average batch size metrics
    * snappy (by google) good for text based messages like log lines or json docs
* **buffer.memmory**(defaults 32MB)  
    * if the producer is way faster then the broker and the buffer is full .send() will start to block
    * the wait is controlled by **max.block.ms**(60000), the time .send() will block
    before throwing an exception 
    
## Consumer Poll Behavior 
* **fetch.min.bytes** (default 1) 
    * controlls how much data you want to pull at least on each request
* **max.poll.records** (default 500)
    * controlls how many records to receive per poll request
* **max.partitions.fetch.bytes** (default 1MB) 
    * Maximum data returned by the broker per partition
* **fetch.max.bytes** (default 50MB)
    * Maximum data returned for each fetch request(covers multiple partitions)
    
## Consumer Liveliness

Consumet talks to kafka(Broker) with poll thread, consumer also talks to coordinator(acting broker) 
with heartbeat thread to detect consumers that are down

* **session.timeout.ms**(default 10 seconds) 
    * if no heartbeat is sent during stipulated period, the consumer is considered dead
* **heartbeat.interval.ms**(default 3 seconds)
    * how ofted to send heartbeats (usually 1/3rd of session timeout)
    
* **max.poll.interval.ms**(defaults 5 minutes)
    * maximum amount of time between two .poll() calls before declaring the consumer dead
    
   
    

