package ru.sergey310872.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sergey310872.config.KafkaConsumerConfig;
import ru.sergey310872.dto.SourceMessage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SourceKafkaConsumer implements Source {
    private KafkaConsumer<String, SourceMessage> consumer;
    private final Random random = new Random();
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public SourceKafkaConsumer() {
        this.consumer = new KafkaConsumerConfig().getConsumer();
    }

    public SourceKafkaConsumer(KafkaConsumer<String, SourceMessage> consumer) {
        this.consumer = consumer;
    }

    @Override
    public Iterable<SourceMessage> source() {
        ConsumerRecords<String, SourceMessage> records = consumer.poll(Duration.ofMillis(100));

        List<SourceMessage> valuesList = new ArrayList<>();
        for (ConsumerRecord<String, SourceMessage> record : records) {
            valuesList.add(record.value());
            LOGGER.info("Received message: key = {}, value = {}", record.key(), record.value());
        }
        return valuesList;
    }
}
