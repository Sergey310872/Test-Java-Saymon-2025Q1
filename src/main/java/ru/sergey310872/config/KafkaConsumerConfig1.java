package ru.sergey310872.config;

//import com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.sergey310872.dto.SourceMessage;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerConfig1 {
    private KafkaConsumer<String, SourceMessage> consumer;

    public KafkaConsumerConfig1() {
        Properties properties = PropertiesFile.PROP;
        Properties props = new Properties();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                properties.getProperty("bootstrap.servers", "localhost:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getProperty("group.id","group1"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
//        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        this.consumer = new KafkaConsumer<>(props);
        String topic = properties.getProperty("kafka.source.topic", "SOURCE");
        consumer.subscribe(Collections.singletonList(topic));

//        String topic = properties.getProperty("kafka.source.topic", "SOURCE");
//        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaConsumer<String, SourceMessage> getConsumer() {
        return consumer;
    }
}
