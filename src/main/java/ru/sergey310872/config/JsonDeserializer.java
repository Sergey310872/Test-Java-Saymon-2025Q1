package ru.sergey310872.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import ru.sergey310872.dto.SourceMessageImp;

public class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Class<T> targetType;

    public JsonDeserializer() {
        this.targetType = (Class<T>) SourceMessageImp.class;
    }

    public JsonDeserializer(Class<T> targetType) {
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
//            throw new RuntimeException("Failed to deserialize JSON", e);
        }
    }
}
