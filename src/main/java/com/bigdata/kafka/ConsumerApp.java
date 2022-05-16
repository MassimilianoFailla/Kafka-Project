package com.bigdata.kafka;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerApp {
    public static void main(String[] args) {


        Long minutes = 10000L;
        Properties config = new Properties();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, new StringDeserializer().getClass().getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, new StringDeserializer().getClass().getName());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdataTeam");
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, "sampleConsumer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(config);

        kafkaConsumer.subscribe(Arrays.asList("search"));

        System.out.println("Sono prima del while ed i valori sono i seguenti:\n");
        System.out.println("BOOTSTRAP_SERVERS_CONFIG -> " +  config.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        System.out.println("Sono prima del while ed i valori sono i seguenti:");
        System.out.println("Sono prima del while ed i valori sono i seguenti:");
        


        while (true){
            System.out.println("Son dentro il while \n");
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ZERO);

            System.out.println("Valore di record -> " + records.toString());
            for (ConsumerRecord<String, String> rec : records){
                System.out.println(rec.value());

            }

        }

    }
}
