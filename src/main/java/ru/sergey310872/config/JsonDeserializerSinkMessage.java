package ru.sergey310872.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import ru.sergey310872.dto.SinkMessageImp;

public class JsonDeserializerSinkMessage<T> implements Deserializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Class<T> targetType;

    public JsonDeserializerSinkMessage() {
        this.targetType = (Class<T>) SinkMessageImp.class;
    }

    public JsonDeserializerSinkMessage(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return objectMapper.readValue(bytes, targetType);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
