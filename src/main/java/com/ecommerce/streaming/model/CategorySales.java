package com.ecommerce.streaming.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Mutable aggregation result for a product category within a tumbling time window.
 * Used as the value type in the Kafka Streams windowed KTable ("category-sales-store").
 *
 * Kafka Streams requires:
 *  - A no-arg constructor for the initializer lambda
 *  - Mutable state (updated in the aggregator lambda)
 *
 * <p>All monetary fields are {@link BigDecimal}. With {@code double}, the running
 * {@code totalSales += amount} accumulated binary rounding error on every record in
 * the window, so a busy window drifted away from the true SUM (and ksqlDB's answer)
 * by a few cents. BigDecimal addition is exact; results are normalised to 2 decimal
 * places with {@link RoundingMode#HALF_UP}.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CategorySales {

    /** Money scale / rounding applied to every derived monetary value. */
    private static final int MONEY_SCALE = 2;
    private static final RoundingMode MONEY_ROUNDING = RoundingMode.HALF_UP;

    private String category;
    private long orderCount;
    private BigDecimal totalSales = BigDecimal.ZERO.setScale(MONEY_SCALE);
    private int totalQuantity;
    private BigDecimal avgOrderValue = BigDecimal.ZERO.setScale(MONEY_SCALE);
    private BigDecimal maxOrderValue = BigDecimal.ZERO.setScale(MONEY_SCALE);

    /**
     * Smallest order seen in this window; {@code null} until the first order arrives.
     * (The old {@code double} version seeded this with {@code Double.MAX_VALUE}, which
     * leaked a nonsense sentinel into the JSON of any empty/never-updated bucket.)
     */
    private BigDecimal minOrderValue;

    /**
     * Aggregate a new order into this bucket.
     * Called by the Kafka Streams aggregator on every matching record.
     */
    public CategorySales update(String category, BigDecimal amount, int quantity) {
        BigDecimal value = money(amount == null ? BigDecimal.ZERO : amount);

        this.category = category;
        this.orderCount++;
        this.totalSales = money(this.totalSales == null ? value : this.totalSales.add(value));
        this.totalQuantity += quantity;
        this.avgOrderValue = this.totalSales.divide(
                BigDecimal.valueOf(this.orderCount), MONEY_SCALE, MONEY_ROUNDING);

        if (this.maxOrderValue == null || value.compareTo(this.maxOrderValue) > 0) {
            this.maxOrderValue = value;
        }
        if (this.minOrderValue == null || value.compareTo(this.minOrderValue) < 0) {
            this.minOrderValue = value;
        }
        return this;
    }

    /**
     * Combine two partial aggregates for the SAME (category, window) into one.
     *
     * <p>Used by the multi-instance Interactive Query fan-out in
     * {@code InteractiveQueryService}: a windowed scan collects one bucket per instance and
     * has to reduce them into a single answer. Under the standard partitioning a given
     * (category, window) lives on exactly one instance, so this is normally a no-op — but
     * "normally" is not "always" (a scan issued mid-rebalance can see the same task on both
     * the old and the new owner), and the previous controller resolved that collision by
     * KEEPING WHICHEVER BUCKET HAD MORE ORDERS and discarding the other. That is silent data
     * loss. Summing is the only answer that is arithmetically defensible.
     *
     * <p>The average is recomputed from the summed totals — averaging two averages would be
     * wrong whenever the two buckets have different order counts. Min/max are folded, not
     * summed, because they are extrema rather than totals.
     */
    public static CategorySales merge(CategorySales left, CategorySales right) {
        if (left == null) return right;
        if (right == null) return left;

        CategorySales merged = new CategorySales();
        merged.category = left.category != null ? left.category : right.category;
        merged.orderCount = left.orderCount + right.orderCount;
        merged.totalSales = money(nullToZero(left.totalSales).add(nullToZero(right.totalSales)));
        merged.totalQuantity = left.totalQuantity + right.totalQuantity;
        merged.avgOrderValue = merged.orderCount == 0
                ? BigDecimal.ZERO.setScale(MONEY_SCALE)
                : merged.totalSales.divide(
                        BigDecimal.valueOf(merged.orderCount), MONEY_SCALE, MONEY_ROUNDING);
        merged.maxOrderValue = maxOf(left.maxOrderValue, right.maxOrderValue);
        merged.minOrderValue = minOf(left.minOrderValue, right.minOrderValue);
        return merged;
    }

    private static BigDecimal maxOf(BigDecimal a, BigDecimal b) {
        if (a == null) return b;
        if (b == null) return a;
        return a.compareTo(b) >= 0 ? a : b;
    }

    private static BigDecimal minOf(BigDecimal a, BigDecimal b) {
        if (a == null) return b;
        if (b == null) return a;
        return a.compareTo(b) <= 0 ? a : b;
    }

    private static BigDecimal nullToZero(BigDecimal v) {
        return v == null ? BigDecimal.ZERO : v;
    }

    private static BigDecimal money(BigDecimal v) {
        return v.setScale(MONEY_SCALE, MONEY_ROUNDING);
    }
}
