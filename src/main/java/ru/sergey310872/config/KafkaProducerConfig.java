package ru.sergey310872.config;

//import com.fasterxml.jackson.databind.JsonSerializer;
//import com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
//import org.springframework.kafka.support.serializer.JsonSerializer;
import ru.sergey310872.dto.SinkMessage;

import java.util.Properties;

public class KafkaProducerConfig {
    private KafkaProducer<String, SinkMessage> producer;// = new KafkaProducer<>(props);

    public KafkaProducerConfig() {
        Properties properties = PropertiesFile.PROP;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                properties.getProperty("bootstrap.servers", "localhost:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
//        props.put("value.serializer", "org.springframework.kafka.support.serializer.JsonSerializer");
        this.producer = new KafkaProducer<>(props);
    }

    public KafkaProducer<String, SinkMessage> getProducer() {
        return producer;
    }
}
