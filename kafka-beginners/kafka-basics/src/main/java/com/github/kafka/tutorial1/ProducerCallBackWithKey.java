/*
 * Copyright (c) Siemens AG 2021 ALL RIGHTS RESERVED.
 *
 * Sensproducts  
 * 
 */

package com.github.kafka.tutorial1;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author z003zp2f
 *
 */
public class ProducerCallBackWithKey
{
    
    private static final Logger logger = LoggerFactory.getLogger(ProducerCallBackWithKey.class);
    
    public static void main(String[] args) throws InterruptedException, ExecutionException
    {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++)
        {
            String key = "id_" + i;
            ProducerRecord<String,
                String> record = new ProducerRecord<String, String>("first_topic", key, "hello world " + Integer.toString(i));
            logger.info("key : " + key);
            kafkaProducer.send(record, new Callback()
            {
                public void onCompletion(RecordMetadata recordMetadata, Exception e)
                {
                    if (e == null)
                    {
                        logger.info("Received new metadata. \n" +
                            "Topic:" + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                    }
                    else
                    {
                        logger.error("Error while producing", e);
                    }
                }
            }).get();
            
        }
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}

/*
 * Copyright (c) Siemens AG 2021 ALL RIGHTS RESERVED
 * Sensproducts
 */
