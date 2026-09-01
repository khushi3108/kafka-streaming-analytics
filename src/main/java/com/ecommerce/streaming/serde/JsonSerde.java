package com.ecommerce.streaming.serde;

import com.fasterxml.jackson.core.JsonGenerator;
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

    /**
     * Shared mapper.
     *
     * <p>Money is modelled as {@link java.math.BigDecimal}, so two Jackson settings matter:
     * <ul>
     *   <li>{@code USE_BIG_DECIMAL_FOR_FLOATS} – any JSON floating-point value is read as a
     *       BigDecimal rather than a lossy double. Typed fields already bind correctly, but
     *       this also protects untyped/Map or JsonNode paths from silently losing precision.</li>
     *   <li>{@code WRITE_BIGDECIMAL_AS_PLAIN} – emit {@code 2499.99}, never {@code 2.49999E+3}.
     *       ksqlDB reads these topics as {@code DOUBLE}; scientific notation is a parsing risk
     *       and makes the payloads unreadable in kafka-console-consumer.</li>
     * </ul>
     */
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
            .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);

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

