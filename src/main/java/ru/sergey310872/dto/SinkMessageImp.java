package ru.sergey310872.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Map;

public class SinkMessageImp implements SinkMessage {
    long from;
    long to;
    transient Map<String, String> labels;
    double min;
    double max;
    double avg;
    int count;

    public SinkMessageImp() {
    }

    public SinkMessageImp(long from, long to, Map<String, String> labels, double min, double max, double avg, int count) {
        this.from = from;
        this.to = to;
        this.labels = labels;
        this.min = min;
        this.max = max;
        this.avg = avg;
        this.count = count;
    }

    @Override
    @JsonProperty("from")
    public long from() {
        return this.from;
    }

    @Override
    @JsonProperty("to")
    public long to() {
        return this.to;
    }

    @Override
    @JsonProperty("labels")
    public Map<String, String> labels() {
        return this.labels;
    }

    @Override
    @JsonProperty("min")
    public double min() {
        return this.min;
    }

    @Override
    @JsonProperty("max")
    public double max() {
        return this.max;
    }

    @Override
    @JsonProperty("avg")
    public double avg() {
        return this.avg;
    }

    @Override
    @JsonProperty("count")
    public int count() {
        return this.count;
    }
}
