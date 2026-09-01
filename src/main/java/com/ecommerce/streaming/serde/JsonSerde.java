package com.ecommerce.streaming.serde;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

/**
 * Generic Kafka Serde that uses Jackson for JSON serialization/deserialization.
 *
 * <p>Usage in Kafka Streams topology:
 * <pre>
 *   Serde&lt;Order&gt; orderSerde = new JsonSerde&lt;&gt;(Order.class);
 *   KStream&lt;String, Order&gt; stream = builder.stream("orders",
 *       Consumed.with(Serdes.String(), orderSerde));
 * </pre>
 *
 * @param <T> The target POJO type
 */
public class JsonSerde<T> implements Serde<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final Class<T> targetType;

    public JsonSerde(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Error serializing JSON for topic [" + topic + "]", e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null || bytes.length == 0) return null;
            try {
                return MAPPER.readValue(bytes, targetType);
            } catch (IOException e) {
                throw new SerializationException(
                        "Error deserializing JSON for topic [" + topic + "] into " + targetType.getSimpleName(), e);
            }
        };
    }
}

