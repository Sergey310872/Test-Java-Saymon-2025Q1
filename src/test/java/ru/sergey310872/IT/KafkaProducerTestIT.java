package ru.sergey310872.IT;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.sergey310872.config.JsonDeserializerSinkMessage;
import ru.sergey310872.config.JsonSerializer;
import ru.sergey310872.config.PropertiesFile;
import ru.sergey310872.dto.SinkMessage;
import ru.sergey310872.dto.SinkMessageImp;
import ru.sergey310872.service.SinkKafkaProducer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;


public class KafkaProducerTestIT {
    private static Properties PROPERTY;
    private static String TOPIC;

    private KafkaConsumer<String, SinkMessage> consumer;
    private KafkaProducer<String, SinkMessage> producer;

    private SinkKafkaProducer sinkKafkaProducer;

    @BeforeAll
    static void prepare() {
        PROPERTY = new Properties();
        try (InputStream input = PropertiesFile.class.getClassLoader()
                .getResourceAsStream("application-test.properties")) {
            PROPERTY.load(input);
        } catch (Exception e) {
            e.printStackTrace();
        }

        TOPIC = PROPERTY.getProperty("kafka.produce.topic", "test-produce-topic");
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
        Properties consumerProps1 = new Properties();
        consumerProps1.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                PROPERTY.getProperty("bootstrap.servers", "localhost:9092"));
        consumerProps1.put(ConsumerConfig.GROUP_ID_CONFIG, PROPERTY.getProperty("group.id", "group1"));
        consumerProps1.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps1.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializerSinkMessage.class.getName());

        consumer = new KafkaConsumer<>(consumerProps1);
        consumer.subscribe(Collections.singletonList(TOPIC));

        sinkKafkaProducer = new SinkKafkaProducer(producer, PROPERTY);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
        producer.close();
    }

    @Test
    void sendMessageToKafka() {
        //Готовим сообщение
        Map<String, String> testMap = new HashMap<>();
        testMap.put("key", "volume produced");
        SinkMessage sinkMessage = new SinkMessageImp(11113, 22222, testMap, 1, 10, 5, 3);

        // Отправляем сообщение
        sinkKafkaProducer.accept(sinkMessage);

        // Читаем сообщение
        ConsumerRecords<String, SinkMessage> received1 = consumer.poll(Duration.ofSeconds(10));
        ConsumerRecord<String, SinkMessage> received = received1.iterator().next();

        // Проверяем
        assertThat(received.value()).isEqualTo(sinkMessage);
    }
}
