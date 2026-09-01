package com.ecommerce.streaming.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Fraud / anomaly alert generated when an order exceeds the configured threshold.
 * Published to the "fraud-alerts" Kafka topic.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderAlert {

    private String alertId;
    private String orderId;
    private String customerId;
    private String productId;
    private String productName;
    private String category;
    /** Order amount in USD – BigDecimal to stay consistent with Order/EnrichedOrder. */
    private BigDecimal amount;
    private int quantity;

    /**
     * Alert type:
     *  HIGH_VALUE_ORDER  – single order exceeds fraud threshold
     */
    private String alertType;

    /**
     * Severity based on amount:
     *  CRITICAL (> $2000), HIGH (> $1000), MEDIUM (> $500)
     */
    private String severity;
    private long timestamp;

    private static final BigDecimal SEVERITY_CRITICAL = new BigDecimal("2000");
    private static final BigDecimal SEVERITY_HIGH     = new BigDecimal("1000");

    /** Build an alert from an EnrichedOrder */
    public static OrderAlert fromEnrichedOrder(EnrichedOrder order, String alertType) {
        BigDecimal amount = order.getAmount() != null ? order.getAmount() : BigDecimal.ZERO;

        String severity;
        if (amount.compareTo(SEVERITY_CRITICAL) > 0) severity = "CRITICAL";
        else if (amount.compareTo(SEVERITY_HIGH) > 0) severity = "HIGH";
        else severity = "MEDIUM";

        return OrderAlert.builder()
                .alertId(UUID.randomUUID().toString())
                .orderId(order.getOrderId())
                .customerId(order.getCustomerId())
                .productId(order.getProductId())
                .productName(order.getProductName())
                .category(order.getCategory())
                .amount(amount)
                .quantity(order.getQuantity())
                .alertType(alertType)
                .severity(severity)
                .timestamp(System.currentTimeMillis())
                .build();
    }
}

