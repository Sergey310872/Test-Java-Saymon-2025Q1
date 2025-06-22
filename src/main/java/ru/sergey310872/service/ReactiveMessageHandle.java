package ru.sergey310872.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import ru.sergey310872.config.KafkaConsumerConfig;
import ru.sergey310872.config.KafkaProducerConfig;
import ru.sergey310872.config.PropertiesFile;
import ru.sergey310872.dto.SinkMessage;
import ru.sergey310872.dto.SinkMessageImp;
import ru.sergey310872.dto.SourceMessage;
import ru.sergey310872.dto.SourceMessageImp;

import java.time.Duration;
import java.util.*;

public class ReactiveMessageHandle {
    private final KafkaConsumerConfig kafkaConsumerConfig;
    private final KafkaProducerConfig kafkaProducerConfig;
    private final Properties properties;


    public ReactiveMessageHandle() {
        this.kafkaConsumerConfig = new KafkaConsumerConfig();
        this.kafkaProducerConfig = new KafkaProducerConfig();
        this.properties = PropertiesFile.PROP;
    }

    public void MessageHandle() {
        // готовим consumer
        ReceiverOptions<String, SourceMessage> receiverOptions =
                ReceiverOptions.<String, SourceMessage>create(kafkaConsumerConfig.getProps())
                        .subscription(Collections.singleton(kafkaConsumerConfig.getTopic()));
        // готовим producer
        SenderOptions<String, SinkMessage> senderOptions = SenderOptions.create(kafkaProducerConfig.getProps());
        KafkaSender<String, SinkMessage> sender = KafkaSender.create(senderOptions);

        String topic = properties.getProperty("kafka.produce.topic", "SINK");
        String key = properties.getProperty("kafka.produce.key", "key1");
        Integer timeFrame = Integer.valueOf(properties.getProperty("time.frame.s", "1"));

        sender.send(KafkaReceiver.create(receiverOptions)
                        .receiveAutoAck()
                        .log("Logging from Flux")
                        .concatMap(consumerRecordFlux -> consumerRecordFlux)
                        .map(ConsumerRecord::value)
                        .distinct() // дедупликация
                        .filter(this::filtering) // фильтрация
                        .window(Duration.ofSeconds(timeFrame))// разбиваем по временным окнам
                        .flatMap(w -> {
                            return w.reduce(new ArrayList<>(), (a, b) -> {
                                a.add(b);
                                return a;
                            });
                        })
                        .flatMap(a -> Flux.fromIterable(groupAndAggregation(a))) // группировка и агрегация
                        .doOnNext(r -> System.out.println("   * 'sink: " + r))
                        .map(s -> SenderRecord.create(new ProducerRecord<>(topic, key, s), "correlation " + s)))
                .doOnError(e -> System.err.println("Send failed: " + e))
                .doOnNext(r -> System.out.println("Message sent: " + r.correlationMetadata()))
                .subscribe();

    }

    public boolean filtering(SourceMessage sourceMessage) {
        double value = sourceMessage.value();
        if (value < Double.parseDouble(properties.getProperty("filter.value.min", "0"))) {
            return false;
        }
        if (value > Double.parseDouble(properties.getProperty("filter.value.max", "100"))) {
            return false;
        }
        return true;
    }

    public Iterable<SinkMessage> groupAndAggregation(Iterable<Object> source) {
        Map<String, SinkMessage> groupedToSink = new HashMap<>();
        String[] strKeys = this.properties.getProperty("keys.for.group.by", "").split(",");
        Set<String> keySetGroupBy = new HashSet<>(Arrays.asList(strKeys));
        SourceMessage sourceMessage;

        for (Object sourceMessage1 : source) {
            sourceMessage = (SourceMessageImp) sourceMessage1;
            Map<String, String> labels = sourceMessage.labels();
            StringBuilder sb = new StringBuilder();
            for (String key : keySetGroupBy) {
                sb.append(key);
                sb.append(labels.get(key));
            }
            String keyGroup = sb.toString();
            SinkMessageImp sinkMessage = (SinkMessageImp) groupedToSink.get(keyGroup);
            if (sinkMessage == null) {
                groupedToSink.put(keyGroup, new SinkMessageImp(sourceMessage.timestamp(),
                        sourceMessage.timestamp(),
                        new HashMap<>(sourceMessage.labels()),
                        sourceMessage.value(),
                        sourceMessage.value(),
                        sourceMessage.value(),
                        1));
            } else {
                sinkMessage.setFrom(Math.min(sinkMessage.from(), sourceMessage.timestamp()));
                sinkMessage.setTo(Math.max(sinkMessage.to(), sourceMessage.timestamp()));
                sinkMessage.labels().putAll(sourceMessage.labels());
                sinkMessage.setMin(Math.min(sinkMessage.min(), sourceMessage.value()));
                sinkMessage.setMax(Math.max(sinkMessage.max(), sourceMessage.value()));
                sinkMessage.setAvg((sinkMessage.avg() * sinkMessage.count() + sourceMessage.value()) / (sinkMessage.count() + 1));
                sinkMessage.setCount(sinkMessage.count() + 1);
            }
        }
        return groupedToSink.values();
    }
}
