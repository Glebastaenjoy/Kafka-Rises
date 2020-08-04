package com.github.prasun.kafkamain;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class producerDemoWithKey {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(producerDemoWithKey.class);
        //Step 1: Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Step 2: Create The Producer and Record
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i <= 20; i++) {
            String topic = "myfirst_topic";
            String value = "Hello From Kafka" + Integer.toString(i);
            final String key = "id_" + Integer.toString(i);
            //Key always goes to the same partition when number of partitions are same
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);
            //Step 3: Send Data- Asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata" +
                                "\nTopic:" + recordMetadata.topic() +
                                "\nPartitions:" + recordMetadata.partition() +
                                "\nOffset:" + recordMetadata.offset() +
                                "\nKey:"+key+
                                "\nTimestamp:" + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing:", e);
                    }
                }
            });
        }
        producer.flush();

        producer.close();
    }
}
