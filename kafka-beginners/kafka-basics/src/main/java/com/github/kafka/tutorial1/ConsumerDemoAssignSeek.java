/*
 * Copyright (c) Siemens AG 2021 ALL RIGHTS RESERVED.
 *
 * Sensproducts  
 * 
 */

package com.github.kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author z003zp2f
 *
 */
public class ConsumerDemoAssignSeek
{
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
    
    public static void main(String[] args)
    {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        long offsetreadfrom = 15L;
        TopicPartition topicPartition = new TopicPartition("first_topic", 0);
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seek(topicPartition, offsetreadfrom);
//        consumer.subscribe(Collections.singleton("first_topic"));
        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;
        while (true)
        {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords)
            {
                numberOfMessagesReadSoFar += 1;
                LOGGER.info("key : " + consumerRecord.key() + " value " + consumerRecord.value() + "\n");
                LOGGER.info("partition : " + consumerRecord.partition() + " offset " + consumerRecord.offset() + "\n");
                
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead)
                {
                    keepOnReading = false; // to exit the while loop
                    break; // to exit the for loop
                }
            }
        }
    }
}

/*
 * Copyright (c) Siemens AG 2021 ALL RIGHTS RESERVED
 * Sensproducts
 */
