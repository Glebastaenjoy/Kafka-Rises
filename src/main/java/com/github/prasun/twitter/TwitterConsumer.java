package com.github.prasun.twitter;

import com.github.prasun.kafkamain.consumerDemo;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.DBObject;
import com.mongodb.MongoWriteException;
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
import java.util.HashMap;
import java.util.Properties;

public class TwitterConsumer {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(consumerDemo.class);
        MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
        MongoDatabase db = mongoClient.getDatabase("tweets");
        MongoCollection<Document> tweetsCollection = db.getCollection("rhea");
        KafkaConsumer<String,String> consumer = createConsumer("tweets");
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            logger.info("Received:"+records.count()+ " records");
            for (ConsumerRecord<String,String> record: records){
                logger.info("Key:"+record.key()+" Value:"+record.value());
                logger.info("Partition:"+record.partition()+", Offset:"+record.offset());
                // Unique id for Idempotent Consumer
                // Kafka Generic Id -> record.topic()+"_"+record.partition()+"_"+record.offset();
                // Stream specific id:
                HashMap<String,String> tweetRecs = extractIdFromTweet(record.value());
                Document tweet = new Document("_id",tweetRecs.get("id_str"));
                tweet.append("data",Document.parse(record.value()));
                tweet.append("name",tweetRecs.get("name"));
                tweet.append("text",tweetRecs.get("text"));
                try {
                    tweetsCollection.insertOne(tweet);
                }catch (MongoWriteException e){
                    e.printStackTrace();
                }

            }
            consumer.commitSync();
            logger.info("Offsets Commited");
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
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"20");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }
    private static final JsonParser jsonParser = new JsonParser();
    private static HashMap<String, String>  extractIdFromTweet(String tweetJson){
        // Using Gson library
        JsonObject jsonObject=  jsonParser.parse(tweetJson).getAsJsonObject();
        HashMap<String,String> tweetIds = new HashMap<>();
        tweetIds.put("id_str",jsonObject.get("id_str").getAsString());
        tweetIds.put("text",jsonObject.get("text").getAsString());
        tweetIds.put("name",jsonObject.getAsJsonObject("user").get("name").getAsString());
        return tweetIds;
    }
}
