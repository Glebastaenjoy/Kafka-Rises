# Basic Configuration

## Producer Configurations

#### **Idempotence Producer**
* **acks:** 
    * acks=0 :- no response requested (Lowest latency and safety)
    * acks=1(default 2.0.0) :- Leader response is requested
    * acks=all :- Leader + Replicas acks requested (Highest latency and safety)
        * acks=all must be used in conjunction with  **min.insync.replicas**.
        It can be set at broker level or overriden at topic level
        * **min.insync.replicas**=2 implies that at least 2 ISR(including leader)
        must respond that they have the data.
        * If you have **replication.factor**=3, min.insync.replicas=2 and acks=all
        producer can tolerate one brocker going down.
        
* **retries** 
    * Defaults to 0
    * can be assigned high values such as Integers.Max_value
    
* **max.in.flight.requests.per.connection**
    * Defaults to 5
    * Set to 1 to ensure ordering
    
* set **enable.idempotence** to true, it automatically sets
    * retries=Integer.MAC_VALUE
    * max.in.flight.requests=5
    * acks=all
* might impact throughput and latancy
        
## Consumer Configuration

* **Delivery Semantics**
    * At most once : offsets are committed as soon as the message batch is received
    * At least once : offsets are committed after the message is processed.
        * If the process goes wrong data will be read again
        * data are safe but might get duplicated
        * make sure to have idempotent processing
    * Exactly once : can be achieved for kafka => kafka workflows using Kafka Streams API
    
* committing offsets
    * **enable.auto.commit=true** and synchronous processing of batches
        * offsets commited autmatically **auto.commit.interval.ms**(default 5000)
        every time you call .poll()
        * if processing is not synchronous it will behave as  'at-most-once'
    * **enable.auto.commit=false** and manual commits of offsets
        * You controll when you commit offsets and what's the condition for commiting
     