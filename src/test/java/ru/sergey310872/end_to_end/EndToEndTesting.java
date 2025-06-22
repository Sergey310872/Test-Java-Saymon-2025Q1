package ru.sergey310872.end_to_end;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
import ru.sergey310872.config.JsonDeserializerSinkMessage;
import ru.sergey310872.config.JsonSerializer;
import ru.sergey310872.config.PropertiesFile;
import ru.sergey310872.dto.SinkMessage;
import ru.sergey310872.dto.SinkMessageImp;
import ru.sergey310872.dto.SourceMessage;
import ru.sergey310872.dto.SourceMessageImp;
import ru.sergey310872.service.SinkKafkaProducer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class EndToEndTesting {
    private static Properties PROPERTY;
//    private static String TOPIC_PRODUCER;
//    private static String TOPIC_CONSUMER;

    private KafkaConsumer<String, SinkMessage> consumer;
    private KafkaProducer<String, SourceMessage> producer;

    private SinkKafkaProducer sinkKafkaProducer;

    @BeforeAll
    static void prepare() {
        PROPERTY = new Properties();
        try (InputStream input = PropertiesFile.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            PROPERTY.load(input);
        } catch (Exception e) {
            e.printStackTrace();
        }

//        TOPIC_PRODUCER = PROPERTY.getProperty("kafka.source.topic", "test-topic");
//        TOPIC_CONSUMER = PROPERTY.getProperty("kafka.produce.topic", "test-produce-topic");
    }

    @BeforeEach
    void setUp() throws IOException {

        // Настройка Producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                PROPERTY.getProperty("bootstrap.servers", "localhost:9092"));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        producer = new KafkaProducer<String, SourceMessage>(producerProps);

        // Настройка Consumer
        Properties consumerProps1 = new Properties();
        consumerProps1.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                PROPERTY.getProperty("bootstrap.servers", "localhost:9092"));
        consumerProps1.put(ConsumerConfig.GROUP_ID_CONFIG, PROPERTY.getProperty("group.id", "group1"));
        consumerProps1.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps1.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializerSinkMessage.class.getName());

        consumer = new KafkaConsumer<>(consumerProps1);
        consumer.subscribe(Collections.singletonList(PROPERTY.getProperty("kafka.produce.topic", "test-produce-topic")));
//
//        sinkKafkaProducer = new SinkKafkaProducer(producer, PROPERTY);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
        producer.close();
    }

    @Test
    void sendMessageToKafka() throws InterruptedException {
        //Готовим сообщение
//        Map<String, String> testMap = new HashMap<>();
//        testMap.put("key", "volume produced");
//        SinkMessage sinkMessage = new SinkMessageImp(11113, 22222, testMap, 1, 10, 5, 3);
        List<SourceMessage> serviceMessageList = new ArrayList<>();
        serviceMessageList.add(new SourceMessageImp(11111, Map.of("A", "value A"), 30));
        serviceMessageList.add(new SourceMessageImp(11112, Map.of("A", "value A"), 40));
        serviceMessageList.add(new SourceMessageImp(11113, Map.of("A", "value B"), 50));
        serviceMessageList.add(new SourceMessageImp(11114, Map.of("B", "value B"), 60));
        serviceMessageList.add(new SourceMessageImp(11115, Map.of("B", "value B"), 70));
        serviceMessageList.add(new SourceMessageImp(11116, Map.of("A", "value A"), 40));
        serviceMessageList.add(new SourceMessageImp(11117, Map.of("A", "value B"), 50));
        serviceMessageList.add(new SourceMessageImp(11118, Map.of("B", "value B"), 60));
        serviceMessageList.add(new SourceMessageImp(11119, Map.of("A", "value A"), 40));
        serviceMessageList.add(new SourceMessageImp(11120, Map.of("A", "value B"), 50));
        serviceMessageList.add(new SourceMessageImp(11121, Map.of("B", "value B"), 60));

//        List<SinkMessage> expected = new ArrayList<>();
        List<SinkMessage> expected = new ArrayList<>();
        expected.add(new SinkMessageImp(11114, 11115, Map.of("B", "value B"), 60, 70, 65, 2));
        expected.add(new SinkMessageImp(11118, 11118, Map.of("B", "value B"), 60, 60, 60, 1));
        expected.add(new SinkMessageImp(11121, 11121, Map.of("B", "value B"), 60, 60, 60, 1));


        // Отправляем сообщение
//        sinkKafkaProducer.accept(sinkMessage);
        String topic = PROPERTY.getProperty("kafka.source.topic", "SOURCE");
        String key = PROPERTY.getProperty("kafka.produce.key", "key1");

        for (SourceMessage serviceMessage : serviceMessageList) {
            ProducerRecord<String, SourceMessage> record = new ProducerRecord<>(topic, key, serviceMessage);
            producer.send(record);
            System.out.println("        send: " + serviceMessage);
            Thread.sleep(2000);
        }

        // Читаем сообщение
        ConsumerRecords<String, SinkMessage> received = consumer.poll(Duration.ofSeconds(100));
        List<SinkMessage> valuesList = new ArrayList<>();
        for (ConsumerRecord<String, SinkMessage> record : received) {
            valuesList.add(record.value());
        }



//        ConsumerRecord<String, SinkMessage> received = received1.iterator().next();
        System.out.println("----------------------------- test ----------------------------");
        for (SinkMessage serviceMessage : valuesList) {
            System.out.println(serviceMessage);
        }

        // Проверяем
        assertNotNull(valuesList);
        assertNotSame(serviceMessageList, valuesList);
        assertEquals(expected.get(0), valuesList.get(0));
        assertEquals(expected.get(1), valuesList.get(1));
        assertEquals(expected.get(2), valuesList.get(2));
//        assertEquals(expected.get(1), valuesList.get(1));
//        assertIterableEquals(expected, valuesList);

//        assertThat(received.value()).isEqualTo(sinkMessage);
    }


}
