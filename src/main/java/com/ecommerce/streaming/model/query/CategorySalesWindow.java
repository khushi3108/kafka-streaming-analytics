package com.ecommerce.streaming.model.query;

import com.ecommerce.streaming.model.CategorySales;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * ONE (category, window) bucket from {@code category-sales-store}, flattened for the REST API.
 *
 * <p><b>Why the window bounds are part of the response.</b> The store is keyed by
 * {@code Windowed<String>} — a (category, windowStart, windowEnd) triple — but the previous
 * controller returned a {@code Map<String, CategorySales>} keyed by category alone and
 * collapsed collisions with
 * {@code results.merge(cat, v, (a, b) -> b.getOrderCount() > a.getOrderCount() ? b : a)}.
 * A 2-minute query spans two 1-minute tumbling windows, so every category with activity in
 * both windows silently reported ONE of them and threw the other away. Keying the response by
 * the full window identity is what keeps distinct windows distinct.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class CategorySalesWindow {

    private String category;

    /** Inclusive window start, EVENT time, ISO-8601 UTC. */
    private String windowStart;
    /** Exclusive window end, EVENT time, ISO-8601 UTC. */
    private String windowEnd;
    /** Same bounds in epoch millis, for clients that would rather not parse text. */
    private long windowStartMs;
    private long windowEndMs;

    private long orderCount;
    private BigDecimal totalSales;
    private int totalQuantity;
    private BigDecimal avgOrderValue;
    private BigDecimal maxOrderValue;
    private BigDecimal minOrderValue;

    /** Composite identity used to group buckets across instances during a fan-out merge. */
    public String windowKey() {
        return category + "|" + windowStartMs + "|" + windowEndMs;
    }

    public static CategorySalesWindow of(String category, long startMs, long endMs, CategorySales sales) {
        return CategorySalesWindow.builder()
                .category(category)
                .windowStart(Instant.ofEpochMilli(startMs).toString())
                .windowEnd(Instant.ofEpochMilli(endMs).toString())
                .windowStartMs(startMs)
                .windowEndMs(endMs)
                .orderCount(sales.getOrderCount())
                .totalSales(sales.getTotalSales())
                .totalQuantity(sales.getTotalQuantity())
                .avgOrderValue(sales.getAvgOrderValue())
                .maxOrderValue(sales.getMaxOrderValue())
                .minOrderValue(sales.getMinOrderValue())
                .build();
    }

    /** Round-trip back to the store value type so {@link CategorySales#merge} owns the arithmetic. */
    public CategorySales toAggregate() {
        CategorySales sales = new CategorySales();
        sales.setCategory(category);
        sales.setOrderCount(orderCount);
        sales.setTotalSales(totalSales);
        sales.setTotalQuantity(totalQuantity);
        sales.setAvgOrderValue(avgOrderValue);
        sales.setMaxOrderValue(maxOrderValue);
        sales.setMinOrderValue(minOrderValue);
        return sales;
    }

    /**
     * Merge two buckets for the same (category, window). SUMS — never picks one.
     * See {@link CategorySales#merge} for why.
     */
    public static CategorySalesWindow merge(CategorySalesWindow left, CategorySalesWindow right) {
        if (left == null) return right;
        if (right == null) return left;
        return of(left.category, left.windowStartMs, left.windowEndMs,
                CategorySales.merge(left.toAggregate(), right.toAggregate()));
    }
}
