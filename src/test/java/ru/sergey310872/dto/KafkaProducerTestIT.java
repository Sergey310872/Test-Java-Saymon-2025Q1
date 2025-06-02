package ru.sergey310872.dto;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.sergey310872.config.JsonDeserializer;
import ru.sergey310872.config.JsonSerializer;
import ru.sergey310872.service.SourceKafkaConsumer;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Properties;


public class KafkaProducerTestIT {
    private static final String TOPIC = "test-topic";//"SOURCE";// "test-topic";
    private static final String TEST_MESSAGE = "test3-message-1";

    private KafkaServer kafkaServer;
    private KafkaConsumer<String, SourceMessage> consumer1;
    private KafkaProducer<String, SourceMessage> producer1;

    private SourceKafkaConsumer sourceKafkaConsumer;

    @BeforeEach
    void setUp() throws IOException {

        // Настройка Embedded Kafka
        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", "localhost:2181");
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://localhost:9092");
        brokerProps.setProperty("offsets.topic.replication.factor", "1");

        KafkaConfig config = new KafkaConfig(brokerProps);

        // Настройка Producer
        Properties producerProps1 = new Properties();
        producerProps1.put("bootstrap.servers", "localhost:9092");
        producerProps1.put("key.serializer", StringSerializer.class.getName());
        producerProps1.put("value.serializer", JsonSerializer.class.getName());
        producer1 = new KafkaProducer<>(producerProps1);

        // Настройка Consumer
        Properties consumerProps1 = new Properties();
        consumerProps1.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps1.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
//        consumerProps1.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps1.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps1.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());

        consumer1 = new KafkaConsumer<>(consumerProps1);
        consumer1.subscribe(Collections.singletonList(TOPIC));

        sourceKafkaConsumer = new SourceKafkaConsumer(consumer1);

    }

    @AfterEach
    void tearDown() {
        consumer1.close();
        producer1.close();
//        kafkaServer.shutdown();
    }

    @Test
    void sendMessageToKafka() {

    }
}
