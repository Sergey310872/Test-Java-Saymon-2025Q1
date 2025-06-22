package ru.sergey310872.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.sergey310872.dto.SinkMessage;

import java.util.Properties;

public class KafkaProducerConfig {
    private KafkaProducer<String, SinkMessage> producer;
    private Properties props;


    public KafkaProducerConfig() {
        Properties properties = PropertiesFile.PROP;
        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                properties.getProperty("bootstrap.servers", "localhost:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        this.producer = new KafkaProducer<>(props);
    }

    public KafkaProducer<String, SinkMessage> getProducer() {
        return producer;
    }

    public Properties getProps() {
        return props;
    }
}
