package ru.sergey310872.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

public class SinkMessageImp implements SinkMessage {
    long from;
    long to;
    Map<String, String> labels; //transient
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

    public void setFrom(long from) {
        this.from = from;
    }

    public void setTo(long to) {
        this.to = to;
    }

    public void setLabels(Map<String, String> labels) {
        this.labels = labels;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }

    public void setCount(int count) {
        this.count = count;
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

    @Override
    public String toString() {
        return "SinkMessageImp{" +
                "from=" + from +
                ", to=" + to +
                ", labels=" + labels +
                ", min=" + min +
                ", max=" + max +
                ", avg=" + avg +
                ", count=" + count +
                '}';
    }
}
