package com.github.prasun.twitter;

import com.github.prasun.kafkamain.consumerDemo;
import com.mongodb.DBObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TwitterConsumer {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(consumerDemo.class);
        MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
        MongoDatabase db = mongoClient.getDatabase("kafkaDump");
        MongoCollection<Document> tweetsCollection = db.getCollection("tweets2");
        KafkaConsumer<String,String> consumer = createConsumer("tweets");

        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record: records){
                logger.info("Key:"+record.key()+" Value:"+record.value());
                logger.info("Partition:"+record.partition()+", Offset:"+record.offset());
                Document tweet = Document.parse(record.value());
                tweetsCollection.insertOne(tweet);
                System.out.println("DONE");
            }

        }

    }

    public static KafkaConsumer<String,String> createConsumer(String topic){
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "tweets_groups";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }
}
