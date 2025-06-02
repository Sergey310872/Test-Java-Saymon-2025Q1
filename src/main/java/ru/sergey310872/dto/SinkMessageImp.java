package ru.sergey310872.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SinkMessageImp that = (SinkMessageImp) o;
        return from == that.from && to == that.to && Double.compare(min, that.min) == 0 && Double.compare(max, that.max) == 0 && Double.compare(avg, that.avg) == 0 && count == that.count && Objects.equals(labels, that.labels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to, labels, min, max, avg, count);
    }
}
