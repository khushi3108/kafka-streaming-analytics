package com.ecommerce.streaming.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Mutable aggregation result for a product category within a tumbling time window.
 * Used as the value type in the Kafka Streams windowed KTable ("category-sales-store").
 *
 * Kafka Streams requires:
 *  - A no-arg constructor for the initializer lambda
 *  - Mutable state (updated in the aggregator lambda)
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CategorySales {

    private String category;
    private long orderCount;
    private double totalSales;
    private int totalQuantity;
    private double avgOrderValue;
    private double maxOrderValue;
    private double minOrderValue = Double.MAX_VALUE;

    /**
     * Aggregate a new order into this bucket.
     * Called by the Kafka Streams aggregator on every matching record.
     */
    public CategorySales update(String category, double amount, int quantity) {
        this.category = category;
        this.orderCount++;
        this.totalSales += amount;
        this.totalQuantity += quantity;
        this.avgOrderValue = this.totalSales / this.orderCount;
        if (amount > this.maxOrderValue) this.maxOrderValue = amount;
        if (amount < this.minOrderValue) this.minOrderValue = amount;
        return this;
    }
}

