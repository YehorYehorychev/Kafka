package org.yehorychev;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        int numOfRecords = 100;
        for (int i = 0; i < numOfRecords; i++) {
            producer.send(new ProducerRecord<String, String>("numbers", Integer.toString(i), Integer.toString(i)));
        }
        producer.close();
    }
}
