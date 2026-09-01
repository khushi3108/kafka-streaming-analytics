package com.ecommerce.streaming.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Represents an incoming customer order event published to the "orders" Kafka topic.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class Order {

    /** Unique order identifier (UUID) */
    private String orderId;

    /** Customer who placed the order */
    private String customerId;

    /** Product being ordered */
    private String productId;

    /** Product category (e.g., Electronics, Clothing) - denormalized for quick grouping */
    private String category;

    /**
     * Total order amount in USD.
     * BigDecimal (not double): money must never be represented as a binary float —
     * rounding error compounds through the windowed SUM/AVG aggregations.
     */
    private BigDecimal amount;

    /** Number of items ordered */
    private int quantity;

    /** Order status: PENDING, CONFIRMED, CANCELLED */
    @Builder.Default
    private String status = "PENDING";

    /** Event creation time (epoch millis) */
    private long timestamp;
}

