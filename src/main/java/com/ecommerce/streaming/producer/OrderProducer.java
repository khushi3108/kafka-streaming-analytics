package com.ecommerce.streaming.producer;

import com.ecommerce.streaming.model.Order;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Publishes {@link Order} events to the "orders" Kafka topic.
 * Key = orderId (ensures all events for one order land in the same partition).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    private static final String TOPIC = "orders";

    /**
     * Serialise the order to JSON and publish asynchronously.
     *
     * @param order The order to publish
     */
    public void sendOrder(Order order) {
        try {
            String json = objectMapper.writeValueAsString(order);
            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(TOPIC, order.getOrderId(), json);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("❌ Failed to send order [{}]: {}", order.getOrderId(), ex.getMessage());
                } else {
                    log.info("✅ Order sent [{}] → partition={} offset={}",
                            order.getOrderId(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot serialize order: " + order.getOrderId(), e);
        }
    }
}

