package com.ecommerce.streaming.model.query;

import com.ecommerce.streaming.model.CustomerSpending;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * One customer's lifetime spending aggregate, flattened for the REST API.
 *
 * <p>{@code totalSpent} is a scale-2 {@link BigDecimal} straight out of the store. The previous
 * controller declared the store as {@code ReadOnlyKeyValueStore<String, Double>} and applied
 * {@code Math.round(v * 100.0) / 100.0} on the way out — a double round-trip that undid the
 * point of the BigDecimal migration, on top of the {@link ClassCastException} that generic
 * erasure let it compile its way into.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class CustomerSpendingEntry {

    private String customerId;
    private BigDecimal totalSpent;
    private long orderCount;
    private BigDecimal avgOrderValue;
    /** EVENT time (epoch millis) of this customer's most recent order. */
    private long lastOrderTimestamp;

    public static CustomerSpendingEntry of(String customerId, CustomerSpending spending) {
        return CustomerSpendingEntry.builder()
                .customerId(customerId)
                .totalSpent(spending.getTotalSpent())
                .orderCount(spending.getOrderCount())
                .avgOrderValue(spending.getAvgOrderValue())
                .lastOrderTimestamp(spending.getLastOrderTimestamp())
                .build();
    }

    public CustomerSpending toAggregate() {
        CustomerSpending spending = new CustomerSpending();
        spending.setCustomerId(customerId);
        spending.setTotalSpent(totalSpent);
        spending.setOrderCount(orderCount);
        spending.setAvgOrderValue(avgOrderValue);
        spending.setLastOrderTimestamp(lastOrderTimestamp);
        return spending;
    }

    /**
     * Merge two partial aggregates for the same customer. SUMS via
     * {@link CustomerSpending#merge}, which recomputes the average from the combined totals.
     * Under normal partitioning one customer lives on exactly one instance, so this only
     * fires when a scan overlaps a rebalance — but summing is still the only defensible answer.
     */
    public static CustomerSpendingEntry merge(CustomerSpendingEntry left, CustomerSpendingEntry right) {
        if (left == null) return right;
        if (right == null) return left;
        return of(left.customerId, CustomerSpending.merge(left.toAggregate(), right.toAggregate()));
    }
}
