package com.fifteen02.eventstore.config;

import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.format.FormatMapper;
import org.hibernate.type.format.jackson.JacksonJsonFormatMapper;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

public class ToolsJacksonJsonFormatMapper implements FormatMapper {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final FormatMapper delegate = new JacksonJsonFormatMapper();

    @Override
    public <T> T fromString(CharSequence charSequence, JavaType<T> javaType, WrapperOptions wrapperOptions) {
        if (JsonNode.class.equals(javaType.getJavaTypeClass())) {
            try {
                return javaType.cast(objectMapper.readTree(charSequence.toString()));
            } catch (Exception exception) {
                throw new IllegalArgumentException("Could not deserialize JSON to JsonNode", exception);
            }
        }
        return delegate.fromString(charSequence, javaType, wrapperOptions);
    }

    @Override
    public <T> String toString(T value, JavaType<T> javaType, WrapperOptions wrapperOptions) {
        if (JsonNode.class.equals(javaType.getJavaTypeClass())) {
            try {
                return objectMapper.writeValueAsString(value);
            } catch (Exception exception) {
                throw new IllegalArgumentException("Could not serialize JsonNode to JSON", exception);
            }
        }
        return delegate.toString(value, javaType, wrapperOptions);
    }
}
