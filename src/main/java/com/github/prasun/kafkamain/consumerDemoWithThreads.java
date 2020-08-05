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
        new consumerDemoWithThreads().run();
    }

    private consumerDemoWithThreads() {
    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(consumerDemoWithThreads.class);
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "consumer_thread_group";
        String topic = "myfirst_topic";
        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating the consumer thread");
        //create the consumer runnable
        Runnable cunsumerRunnable = new ConsumerRunnable(
                bootstrapServer,
                groupId,
                topic,
                latch
        );
        //start the thread
        Thread consumerThread = new Thread(cunsumerRunnable);
        consumerThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) cunsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }

        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }

    }

    public class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        public ConsumerRunnable(String bootstrapServer,
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
