package ru.sergey310872.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sergey310872.config.KafkaProducerConfig;
import ru.sergey310872.config.PropertiesFile;
import ru.sergey310872.dto.SinkMessage;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SinkKafkaProducer implements Sink {
    private KafkaProducer<String, SinkMessage> producer;
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public SinkKafkaProducer() {
        this.producer = new KafkaProducerConfig().getProducer();
    }

    public SinkKafkaProducer(KafkaProducer<String, SinkMessage> producer) {
        this.producer = producer;
    }

    @Override
    public void accept(SinkMessage sinkMessage) {
        Properties properties = PropertiesFile.PROP;
        String topic = properties.getProperty("kafka.produce.topic", "SINK");
        String key = properties.getProperty("kafka.produce.key", "key1");

        ProducerRecord<String, SinkMessage> record = new ProducerRecord<>(topic, key, sinkMessage);
        producer.send(record);

        LOGGER.info("Message sent to Kafka topic: {}, with key: {}", topic, key);
    }
}
