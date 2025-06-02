package ru.sergey310872.service;

import ru.sergey310872.config.PropertiesFile;
import ru.sergey310872.dto.SinkMessage;
import ru.sergey310872.dto.SinkMessageImp;
import ru.sergey310872.dto.SourceMessage;
import ru.sergey310872.dto.SourceMessageImp;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class ServiceMessageHandle {
    private static Set<String> keySetGroupBy;

    static {
        String[] strKeys = PropertiesFile.PROP.getProperty("keys.for.group.by", "").split(",");
        keySetGroupBy = new HashSet<>(Arrays.asList(strKeys));
    }

    SourceKafkaConsumer sourceKafkaConsumer;
    SinkKafkaProducer sinkKafkaProducer;
    Properties properties;

    public ServiceMessageHandle() {
        this.sourceKafkaConsumer = new SourceKafkaConsumer();
        this.sinkKafkaProducer = new SinkKafkaProducer();
        this.properties = PropertiesFile.PROP;
    }

    public ServiceMessageHandle(Properties properties) {
        this.sourceKafkaConsumer = new SourceKafkaConsumer();
        this.sinkKafkaProducer = new SinkKafkaProducer();
        this.properties = properties;
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
                            .thenApplyAsync(filteringResult -> groupBy(filteringResult))
                            .thenApplyAsync(groupResult -> aggregation(groupResult))
                            .thenApplyAsync(sinkResult -> sinkMessage(sinkResult));
                }
                from = to;
                to = from + timeFrame;
                System.out.println("------------------------" + to);
            }
        }
    }

     public Iterable<SourceMessage> deduplication(Iterable<SourceMessage> source) {
        Set<SourceMessage> deduplication = new HashSet<>();
        for (SourceMessage sourceMessage : source) {
            if (sourceMessage != null) {
                deduplication.add(sourceMessage);
            }
        }
        return deduplication;
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

    public Iterable<SourceMessage> groupBy(Iterable<SourceMessage> source) {
        Map<SourceMessage, SourceMessage> grouped = new TreeMap(new Comparator<SourceMessage>() {
            @Override
            public int compare(SourceMessage o1, SourceMessage o2) {
                for (String key : keySetGroupBy) {
                    String value_o1 = o1.labels().get(key);
                    String value_o2 = o2.labels().get(key);
                    if (value_o1 != null & value_o2 != null && value_o1.equals(value_o2)) {
                        return 0;
                    }
//                    if (value_o1 == null || value_o2 == null || !value_o1.equals(value_o2)) {
//                        return 1;
//                    }
                }
//                return 0;
                return 1;
            }
        });

        for (SourceMessage sourceMessage : source) {
            SourceMessage message = grouped.get(sourceMessage);
            if (message != null) {
                long groupedTimestemp = Math.max(message.timestamp(), sourceMessage.timestamp());
                Map<String, String> groupedLabels = new HashMap<>(message.labels());
                for (Map.Entry<String, String> str : message.labels().entrySet()) {
                    groupedLabels.put(str.getKey(), str.getValue());
                }
                double groupedValue = message.value() + sourceMessage.value();
                SourceMessage groupedMessage = new SourceMessageImp(groupedTimestemp, groupedLabels, groupedValue);
                grouped.put(groupedMessage, groupedMessage);
            } else {
                grouped.put(sourceMessage, sourceMessage);
            }
        }

        return grouped.values();
    }

    public SinkMessage aggregation(Iterable<SourceMessage> source) {
        Map<String, String> labels = new HashMap<>();
        double min = Double.MAX_VALUE;
        double max = -Double.MAX_VALUE;
        double sum = 0;
        double avg = 0;
        int count = 0;
        long from = Long.MAX_VALUE;
        long to = Long.MIN_VALUE;
        for (SourceMessage sourceMessage : source) {
            from = Math.min(sourceMessage.timestamp(), from);
            to = Math.max(sourceMessage.timestamp(), to);
            labels.putAll(sourceMessage.labels());
            double value = sourceMessage.value();
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count++;
        }
        avg = sum / count;
        return new SinkMessageImp(from, to, labels, min, max, avg, count);
    }

    private Boolean sinkMessage(SinkMessage sinkMessage) {
        if (sinkMessage.count() > 0) {
            sinkKafkaProducer.accept(sinkMessage);
            return true;
        }
        return false;
    }


}
