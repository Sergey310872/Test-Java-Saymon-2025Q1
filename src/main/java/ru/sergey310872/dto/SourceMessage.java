package ru.sergey310872.dto;

import java.util.Map;

public interface SourceMessage {
    long timestamp();
    Map<String, String> labels();
    double value();
}
