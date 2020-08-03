package com.github.prasun.kafkamain;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class producerDemoWithCallback {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(producerDemoWithCallback.class);
        //Step 1: Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Step 2: Create The Producer and Record
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i <= 20; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("myfirst_topic", "Hello From Kafka" + Integer.toString(i));
            //Step 3: Send Data- Asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata" +
                                "\nTopic:" + recordMetadata.topic() +
                                "\nPartitions:" + recordMetadata.partition() +
                                "\nOffset:" + recordMetadata.offset() +
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
