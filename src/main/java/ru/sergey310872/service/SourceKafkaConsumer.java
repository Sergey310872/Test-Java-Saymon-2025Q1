package ru.sergey310872.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sergey310872.config.KafkaConsumerConfig;
import ru.sergey310872.dto.SourceMessage;

import java.time.Duration;
import java.util.*;

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
//        Properties properties = PropertiesFile.PROP;
//        String topic = properties.getProperty("kafka.source.topic", "SOURCE");
//        consumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<String, SourceMessage> records = consumer.poll(Duration.ofMillis(100));

        List<SourceMessage> valuesList = new ArrayList<>();
        for (ConsumerRecord<String, SourceMessage> record : records) {
            valuesList.add(record.value());
            LOGGER.info("Received message: key = {}, value = {}", record.key(), record.value());
//            System.out.printf("Received message: key = %s, value = %s%n", record.key(), record.value());

//            Map<String, String> labels = new HashMap<>();
//            labels.put(record.key(), record.value());
//            valuesList.add(new SourceMessageImp(System.currentTimeMillis(), labels, random.nextDouble(100)));
        }
        return valuesList;
    }
}
