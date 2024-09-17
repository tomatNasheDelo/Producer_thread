package com.example;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Main {


    private static final String TOPIC =  "kafka_thread";
    private static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {


        System.out.println("Hello world!");

      

        Thread producerThread= new Thread(Main::produce);
        producerThread.start();


    }



    private static void produce()    {

        Properties props = new Properties();

        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("batch.size", "10");
        props.put("linger.ms", "2000");
        props.put("acks", "1");

        try(Producer<String, String> producer = new KafkaProducer<>(props)){

            for(int i = 200;;i++){
                
                String key = Integer.toString(i);
                String message = "this is message "+Integer.toString(i);

                producer.send(new ProducerRecord<String, String>(TOPIC, key, message));

                System.out.println("sent msg "+ key);
                try{

                      Thread.sleep(1000);

                } catch (Exception e){
                    
                    break;
                }
            }          
        
    }    catch (Exception e){

           System.out.println("Could not start producer :"+ e);
    }
    }
}
