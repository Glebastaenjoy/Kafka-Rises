package com.github.prasun.kafkamain;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class consumerDemoWithThreads {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(consumerDemoWithThreads.class);
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my_firstconsumer_group";
        String topic = "myfirst_topic";
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerThread = new ConsumerThreads(
                bootstrapServer,
                groupId,
                topic,
                latch
                );

    }
    public class ConsumerThreads implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerThreads.class);

        public ConsumerThreads(String bootstrapServer,
                               String groupId,
                               String topic,
                               CountDownLatch latch) {
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//or latest

            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Collections.singleton(topic));
            // (for multiple topics)consumer.subscribe(Arrays.asList("myfirst_topic"));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key:" + record.key() + " Value:" + record.value());
                        logger.info("Partition:" + record.partition() + ", Offset:" + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received Shutdown Signal!!!");
            } finally {
                consumer.close();
                // Tell our main code we are done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // Special method to interrupt consumer.poll()
            // it will throw a WakeUpWException
            consumer.wakeup();
        }
    }
}
