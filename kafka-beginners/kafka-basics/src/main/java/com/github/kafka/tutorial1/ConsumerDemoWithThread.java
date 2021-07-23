/*
 * Copyright (c) Siemens AG 2021 ALL RIGHTS RESERVED.
 *
 * Sensproducts  
 * 
 */

package com.github.kafka.tutorial1;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author z003zp2f
 *
 */
public class ConsumerDemoWithThread
{
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
    
    public static void main(String[] args)
    {
        CountDownLatch latch = new CountDownLatch(1);
        Runnable consumerThread = (new ConsumerDemoWithThread()).new ConsumerThread(latch);
        Thread t = new Thread(consumerThread);
        t.start();
        
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            LOGGER.info("caught shutdown hook");
            ((ConsumerThread) consumerThread).shutdown();
            try
            {
                latch.await();
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
            LOGGER.info("Application has exited");
        }));
        try
        {
            latch.await();
        }
        catch (InterruptedException e)
        {
            LOGGER.info("man is finished");
            
        }
    }
    
    public class ConsumerThread implements Runnable
    {
        private CountDownLatch latch;
        KafkaConsumer<String, String> consumer = null;
        
        /**
         * @param consumer
         * @param latch
         */
        public ConsumerThread(CountDownLatch latch)
        {
            super();
            this.latch = latch;
            
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my_third-app");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Collections.singleton("first_topic"));
        }
        
        /**
         * {@inheritDoc}
         */
        public void run()
        {
            try
            {
                while (true)
                {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords)
                    {
                        LOGGER.info("key : " + consumerRecord.key() + " value " + consumerRecord.value() + "\n");
                        LOGGER.info("partition : " + consumerRecord.partition() + " offset " + consumerRecord.offset() + "\n");
                    }
                }
            }
            catch (WakeupException e)
            {
                LOGGER.info("Exception occured");
            }
            finally
            {
                consumer.close();
                latch.countDown();
            }
        }
        
        public void shutdown()
        {
            consumer.wakeup();
        }
    }
}

/*
 * Copyright (c) Siemens AG 2021 ALL RIGHTS RESERVED
 * Sensproducts
 */
