package ru.sergey310872.dto;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
//import kafka.utils.TestUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import ru.sergey310872.config.JsonDeserializer;
import ru.sergey310872.config.JsonSerializer;
import ru.sergey310872.config.KafkaConsumerConfig;
import ru.sergey310872.service.SourceKafkaConsumer;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;


class KafkaConsumerTestIT {
    private static final String TOPIC = "test-topic";//"SOURCE";// "test-topic";
    private static final String TEST_MESSAGE = "test3-message-1";

    private KafkaServer kafkaServer;
    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, SourceMessage> consumer1;
    private KafkaProducer<String, SourceMessage> producer1;

    private SourceKafkaConsumer sourceKafkaConsumer;
//    private KafkaConsumerConfig kafkaConsumerConfig;

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
//        kafkaServer = TestUtils.createServer(config, Time.SYSTEM);

        // Настройка Producer
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        Properties producerProps1 = new Properties();
        producerProps1.put("bootstrap.servers", "localhost:9092");
        producerProps1.put("key.serializer", StringSerializer.class.getName());
        producerProps1.put("value.serializer", JsonSerializer.class.getName());
        producer1 = new KafkaProducer<>(producerProps1);
        // Настройка Consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(TOPIC));

        Properties consumerProps1 = new Properties();
        consumerProps1.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps1.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
//        consumerProps1.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps1.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps1.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());

        consumer1 = new KafkaConsumer<>(consumerProps1);
        consumer1.subscribe(Collections.singletonList(TOPIC));

//        kafkaConsumerConfig = Mockito.mock(KafkaConsumerConfig.class);
        sourceKafkaConsumer = new SourceKafkaConsumer(consumer1);

    }

    @AfterEach
    void tearDown() {
        consumer.close();
        producer.close();
//        kafkaServer.shutdown();
    }

    @Test
    void testConsumerReceivesMessage() throws ExecutionException, InterruptedException {
        // Отправляем сообщение
        Map<String, String> testMap = new HashMap<>();
        testMap.put("key", "volume");
        SourceMessage sourceMessage = new SourceMessageImp(1748770678704L, testMap, 44);
        ProducerRecord<String, SourceMessage> record = new ProducerRecord<>(TOPIC, sourceMessage);
//        ProducerRecord<String, SinkMessage> record = new ProducerRecord<>(topic, key, sinkMessage);

//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, TEST_MESSAGE);
        producer1.send(record).get();
//        Thread.sleep(5000);
        // Читаем сообщение
//        new ServiceMessageHandle1().sourceMessages();
//        Mockito.when(kafkaConsumerConfig.getConsumer()).thenReturn(consumer1);
//        Iterable<SourceMessage> testResult = new SourceKafkaConsumer1(consumer1).source();
        Iterable<SourceMessage> testResult = sourceKafkaConsumer.source();
//        ConsumerRecords<String, SourceMessage> received1 = consumer1.poll(Duration.ofSeconds(10));
        ConsumerRecord<String, SourceMessage> received = consumer1.poll(Duration.ofSeconds(10)).iterator().next();
//        ConsumerRecord<String, String> received = consumer.poll(Duration.ofSeconds(10)).iterator().next();

        // Проверяем
        assertThat(received.value()).isEqualTo(sourceMessage);
//        assertThat(received.value()).isEqualTo(TEST_MESSAGE);
    }
}