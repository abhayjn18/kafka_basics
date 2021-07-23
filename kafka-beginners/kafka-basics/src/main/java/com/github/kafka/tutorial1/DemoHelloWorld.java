/*
 * Copyright (c) Siemens AG 2021 ALL RIGHTS RESERVED.
 *
 * Sensproducts  
 * 
 */

package com.github.kafka.tutorial1;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author z003zp2f
 *
 */
public class DemoHelloWorld
{
    public static void main(String[] args)
    {
        System.out.println("Hello World");
        
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World");
        
        kafkaProducer.send(record);
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}

/*
 * Copyright (c) Siemens AG 2021 ALL RIGHTS RESERVED
 * Sensproducts
 */
