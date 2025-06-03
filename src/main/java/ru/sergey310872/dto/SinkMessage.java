package ru.sergey310872.dto;

import java.util.Map;

public interface SinkMessage {
    long from();

    long to();

    Map<String, String> labels();

    double min();

    double max();

    double avg();

    int count();
}
