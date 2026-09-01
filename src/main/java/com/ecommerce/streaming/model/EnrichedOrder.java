package com.ecommerce.streaming.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Result of joining an Order with its Product details.
 * Published to the "enriched-orders" Kafka topic by the Kafka Streams topology.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class EnrichedOrder {

    private String orderId;
    private String customerId;
    private String productId;
    private String productName;
    private String brand;
    private String category;
    /** Order amount in USD – BigDecimal to keep money exact. */
    private BigDecimal amount;
    private int quantity;
    private String status;
    private long timestamp;

    /**
     * Factory method: merges an Order with its Product details.
     * Handles null product gracefully (left-join semantics).
     */
    public static EnrichedOrder from(Order order, Product product) {
        return EnrichedOrder.builder()
                .orderId(order.getOrderId())
                .customerId(order.getCustomerId())
                .productId(order.getProductId())
                .productName(product != null ? product.getName() : "Unknown Product")
                .brand(product != null ? product.getBrand() : "Unknown Brand")
                .category(order.getCategory())
                .amount(order.getAmount())
                .quantity(order.getQuantity())
                .status(order.getStatus())
                .timestamp(order.getTimestamp())
                .build();
    }
}

