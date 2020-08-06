package com.github.prasun.kafkamain;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class consumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(consumerDemoAssignSeek.class);
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "myfirst_topic";
        //Create Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");//or latest

        //Create Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        // assign and seek are mostly used to replay data or fetch a specific message

        // assign
        TopicPartition partitionToRead = new TopicPartition(topic,0);
        consumer.assign(Collections.singleton(partitionToRead));

        // seek
        long offsetToRead=15l;
        consumer.seek(partitionToRead,offsetToRead);

        //poll for new data and read only specified number of data
        int count=5;
        while(count>0){
            //deprecated: consumer.poll(100) new in kafka 2.0.0 needs java 1.8+
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record: records){
                logger.info("Key:"+record.key()+" Value:"+record.value());
                logger.info("Partition:"+record.partition()+", Offset:"+record.offset());
                count -= 1;
                if (count==0)
                    break;
            }
        }
        logger.info("Exiting the application");
    }
}
