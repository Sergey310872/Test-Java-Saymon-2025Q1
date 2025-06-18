package ru.sergey310872.service;

import ru.sergey310872.config.PropertiesFile;
import ru.sergey310872.dto.SinkMessage;
import ru.sergey310872.dto.SinkMessageImp;
import ru.sergey310872.dto.SourceMessage;
import ru.sergey310872.dto.SourceMessageImp;

import java.util.*;
import java.util.concurrent.*;

public class ServiceMessageHandle {
    private static final Set<SourceMessage> deduplication;

    static {
        deduplication = ConcurrentHashMap.newKeySet();
        long timeFrame = Long.parseLong(PropertiesFile.PROP.getProperty("deduplication.timeFrame.milliseconds", "50000"));
        // Создаем планировщик с демон-потоком
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true); // Устанавливаем как демон-поток
            return t;
        });

        // Задача будет выполняться каждые 50 секунд после 50 секунды задержки
        scheduler.scheduleAtFixedRate(() -> {
            System.out.println("Выполняем задачу: " + System.currentTimeMillis());
            Iterator<SourceMessage> iterator = deduplication.iterator();
            while (iterator.hasNext()) {
                SourceMessage message = iterator.next();
                if (message.timestamp() < System.currentTimeMillis() - timeFrame) {
                    iterator.remove();
                }
            }
        }, timeFrame, timeFrame, TimeUnit.MILLISECONDS);
    }

    private final Set<String> keySetGroupBy;

    SourceKafkaConsumer sourceKafkaConsumer;
    SinkKafkaProducer sinkKafkaProducer;
    Properties properties;

    public ServiceMessageHandle() {
        this.sourceKafkaConsumer = new SourceKafkaConsumer();
        this.sinkKafkaProducer = new SinkKafkaProducer();
        this.properties = PropertiesFile.PROP;
        String[] strKeys = this.properties.getProperty("keys.for.group.by", "").split(",");
        keySetGroupBy = new HashSet<>(Arrays.asList(strKeys));
    }

    public ServiceMessageHandle(Properties properties) {
        this.sourceKafkaConsumer = new SourceKafkaConsumer();
        this.sinkKafkaProducer = new SinkKafkaProducer();
        this.properties = properties;
        String[] strKeys = this.properties.getProperty("keys.for.group.by", "").split(",");
        keySetGroupBy = new HashSet<>(Arrays.asList(strKeys));
    }

    public void sourceMessages() {
        long timeFrame = Long.parseLong(PropertiesFile.PROP.getProperty("time.frame.s", "1")) * 1000;
        long from = System.currentTimeMillis();
        long to = from + timeFrame;
        while (true) {
            if (System.currentTimeMillis() > to) {
                Iterable<SourceMessage> dataSource = sourceKafkaConsumer.source();
                if (dataSource.iterator().hasNext()) {
                    CompletableFuture.supplyAsync(() -> deduplication(dataSource))
                            .thenApplyAsync(deduplicationResult -> filtering(deduplicationResult))
                            .thenApplyAsync(filteringResult -> groupAndAggregation(filteringResult))
                            .thenApplyAsync(sinkResult -> sinkMessage(sinkResult)
                            );
                }
                from = to;
                to = from + timeFrame;
            }
        }
    }

    public Iterable<SourceMessage> deduplication(Iterable<SourceMessage> source) {
        Set<SourceMessage> deduplicatedSet = new HashSet<>();
        for (SourceMessage sourceMessage : source) {
            if (sourceMessage != null) {
                if (deduplication.add(sourceMessage)) {
                    deduplicatedSet.add(sourceMessage);
                };
            }
        }
        return deduplicatedSet;
    }

    public Iterable<SourceMessage> filtering(Iterable<SourceMessage> source) {
        List<SourceMessage> list = new ArrayList<>();
        for (SourceMessage sourceMessage : source) {
            double value = sourceMessage.value();
            if (value < Double.parseDouble(properties.getProperty("filter.value.min", "0"))) {
                continue;
            }
            if (value > Double.parseDouble(properties.getProperty("filter.value.max", "100"))) {
                continue;
            }
            list.add(sourceMessage);
        }
        return list;
    }

    public Iterable<SinkMessage> groupAndAggregation(Iterable<SourceMessage> source) {
        Map<String, SinkMessage> groupedToSink = new HashMap<>();
        for (SourceMessage sourceMessage : source) {
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

    private Boolean sinkMessage(Iterable<SinkMessage> sinkMessages) {
        Boolean result = false;
        Iterator<SinkMessage> iterator = sinkMessages.iterator();
        while (iterator.hasNext()) {
            sinkKafkaProducer.accept(iterator.next());
            result = true;
        }
        return result;
    }
}
