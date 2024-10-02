package org.example.controller;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.*;

import java.util.Properties;

@RestController
@RequestMapping("/producer")
public class ProducerController {

    private static final String TOPIC_NAME="topic1";
    private static final String BOOTSTRAP_SERVERS = "192.168.254.237:9092";

    private final Producer<String, String> kafkaProducer;

    public ProducerController(){
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.kafkaProducer = new KafkaProducer<>(props);
    }

    @CrossOrigin(origins = "http://localhost:4000")
    @PostMapping("/event")
    public void sendEventToKafka(@RequestBody String eventData) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "userEvent", eventData);
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Error sending message to Kafka: " + exception.getMessage());
            } else {
                System.out.println("Message sent to Kafka, offset: " + metadata.offset());
            }
        });
    }
}
