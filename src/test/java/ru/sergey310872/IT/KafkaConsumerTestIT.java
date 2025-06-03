package ru.sergey310872.IT;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.sergey310872.config.JsonDeserializer;
import ru.sergey310872.config.JsonSerializer;
import ru.sergey310872.config.PropertiesFile;
import ru.sergey310872.dto.SourceMessage;
import ru.sergey310872.dto.SourceMessageImp;
import ru.sergey310872.service.SourceKafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;


class KafkaConsumerTestIT {
    private static Properties PROPERTY;
    private static String TOPIC;

    private KafkaConsumer<String, SourceMessage> consumer;
    private KafkaProducer<String, SourceMessage> producer;

    private SourceKafkaConsumer sourceKafkaConsumer;

    @BeforeAll
    static void prepare() {
        PROPERTY = new Properties();
        try (InputStream input = PropertiesFile.class.getClassLoader()
                .getResourceAsStream("application-test.properties")) {
            PROPERTY.load(input);
        } catch (Exception e) {
            e.printStackTrace();
        }

        TOPIC = PROPERTY.getProperty("kafka.source.topic", "test-topic");
    }

    @BeforeEach
    void setUp() throws IOException {

        // Настройка Producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                PROPERTY.getProperty("bootstrap.servers", "localhost:9092"));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        producer = new KafkaProducer<>(producerProps);

        // Настройка Consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                PROPERTY.getProperty("bootstrap.servers", "localhost:9092"));
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, PROPERTY.getProperty("group.id", "test-group"));
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());

        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(TOPIC));

        sourceKafkaConsumer = new SourceKafkaConsumer(consumer);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
        producer.close();
    }

    @Test
    void testConsumerReceivesMessage() throws ExecutionException, InterruptedException {
        //Готовим сообщение
        Map<String, String> testMap = new HashMap<>();
        testMap.put("key", "volume");
        SourceMessage sourceMessage = new SourceMessageImp(1748770678717L, testMap, 44);

        // Отправляем сообщение
        ProducerRecord<String, SourceMessage> record = new ProducerRecord<>(TOPIC, sourceMessage);
        producer.send(record).get();

        // Читаем сообщение
        Iterable<SourceMessage> testResult = sourceKafkaConsumer.source();

        // Проверяем
        assertThat(testResult.iterator().next().timestamp()).isEqualTo(sourceMessage.timestamp());
    }
}