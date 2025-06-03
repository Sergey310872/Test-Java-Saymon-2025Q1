package ru.sergey310872.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import ru.sergey310872.config.PropertiesFile;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SourceMessageImp implements SourceMessage, Serializable {
    private static Set<String> keySet;

    static {
        String[] strKeys = PropertiesFile.PROP.getProperty("keys.for.deduplication.by", "").split(",");
        keySet = new HashSet<>(Arrays.asList(strKeys));
    }

    private long timestamp;
    private Map<String, String> labels;
    private double value;

    public SourceMessageImp() {
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setLabels(Map<String, String> labels) {
        this.labels = labels;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public SourceMessageImp(long timestamp, Map<String, String> labels, double value) {
        this.timestamp = timestamp;
        this.labels = labels;
        this.value = value;
    }

    @Override
    @JsonProperty("timestamp")
    public long timestamp() {
        return timestamp;
    }

    @Override
    @JsonProperty("labels")
    public Map<String, String> labels() {
        return labels;
    }

    @Override
    @JsonProperty("value")
    public double value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SourceMessageImp that = (SourceMessageImp) o;

        for (String key : keySet) {
            String thatValue = that.labels.get(key);
            String value = labels.get(key);
            if (value == null || thatValue == null || !thatValue.equals(value)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
