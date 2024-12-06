package org.yehorychev;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;

public class Producer {

    public static void main(String[] args) {
        String clientId = "my-producer";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("client.id", clientId);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        int numOfRecords = 100;
        String topic = "strings";
        for (int i = 0; i < numOfRecords; i++) {
            String message = String.format("Producer %s has sent message %s at %s", clientId, i, new Date());
            System.out.println(message);
            producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), message));
        }
        producer.close();
    }
}
