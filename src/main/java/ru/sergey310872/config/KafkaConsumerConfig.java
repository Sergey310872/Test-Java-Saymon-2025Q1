package ru.sergey310872.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.sergey310872.dto.SourceMessage;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerConfig {
    private KafkaConsumer<String, SourceMessage> consumer;

    public KafkaConsumerConfig() {
        Properties properties = PropertiesFile.PROP;
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                properties.getProperty("bootstrap.servers", "localhost:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getProperty("group.id", "group1"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        this.consumer = new KafkaConsumer<>(props);
        String topic = properties.getProperty("kafka.source.topic", "SOURCE");
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaConsumer<String, SourceMessage> getConsumer() {
        return consumer;
    }
}
