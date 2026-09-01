package com.ecommerce.streaming.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Per-customer spending aggregate.
 *
 * <p>This is the value type of the {@code customer-spending-store} state store
 * (read by {@code AnalyticsController} through Interactive Queries) and it is also
 * reused as the value type of the two windowed anomaly-detection aggregates
 * ({@code customer-velocity-store}, {@code customer-session-store}), because a
 * "how much did this customer spend, over how many orders" summary is exactly what
 * each of those windows needs.
 *
 * <p>Replaces the previous {@code Serdes.Double} running total, which forced the
 * aggregator to round-trip through {@code BigDecimal.valueOf(total).add(amount)
 * .doubleValue()} — re-introducing on every record the binary rounding error the
 * BigDecimal migration was supposed to remove. Money is a {@link BigDecimal}
 * normalised to 2 decimal places with {@link RoundingMode#HALF_UP}, matching
 * {@link CategorySales}.
 *
 * <p>Kafka Streams requires a no-arg constructor (for the {@code Initializer}) and
 * mutable state (for the {@code Aggregator}), hence {@code @Data}/{@code @NoArgsConstructor}
 * rather than an immutable record.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CustomerSpending {

    /** Money scale / rounding applied to every derived monetary value. */
    private static final int MONEY_SCALE = 2;
    private static final RoundingMode MONEY_ROUNDING = RoundingMode.HALF_UP;

    private String customerId;

    /** Exact sum of every order amount folded into this aggregate. */
    private BigDecimal totalSpent = BigDecimal.ZERO.setScale(MONEY_SCALE);

    private long orderCount;

    /** {@code totalSpent / orderCount}, the baseline the anomaly detector compares against. */
    private BigDecimal avgOrderValue = BigDecimal.ZERO.setScale(MONEY_SCALE);

    /** EVENT time (epoch millis) of the most recent order folded in — not wall-clock time. */
    private long lastOrderTimestamp;

    /**
     * Fold one order into this aggregate. Called by the Kafka Streams aggregator.
     *
     * @param customerId     grouping key
     * @param amount         order amount; {@code null} is treated as zero but still counted
     * @param eventTimestamp order event time in epoch millis
     */
    public CustomerSpending update(String customerId, BigDecimal amount, long eventTimestamp) {
        BigDecimal value = money(amount == null ? BigDecimal.ZERO : amount);

        this.customerId = customerId;
        this.orderCount++;
        this.totalSpent = money(this.totalSpent == null ? value : this.totalSpent.add(value));
        this.lastOrderTimestamp = Math.max(this.lastOrderTimestamp, eventTimestamp);
        recomputeAverage();
        return this;
    }

    /**
     * Session merger: when two previously separate sessions turn out to belong to the
     * same session (a late record bridges the inactivity gap), Kafka Streams asks us to
     * combine their aggregates.
     *
     * <p>The average is recomputed from the merged totals — averaging two averages would
     * be wrong whenever the two sessions have different order counts.
     */
    public static CustomerSpending merge(CustomerSpending left, CustomerSpending right) {
        if (left == null) return right;
        if (right == null) return left;

        CustomerSpending merged = new CustomerSpending();
        merged.customerId = left.customerId != null ? left.customerId : right.customerId;
        merged.orderCount = left.orderCount + right.orderCount;
        merged.totalSpent = money(nullToZero(left.totalSpent).add(nullToZero(right.totalSpent)));
        merged.lastOrderTimestamp = Math.max(left.lastOrderTimestamp, right.lastOrderTimestamp);
        merged.recomputeAverage();
        return merged;
    }

    private void recomputeAverage() {
        this.avgOrderValue = this.orderCount == 0
                ? BigDecimal.ZERO.setScale(MONEY_SCALE)
                : this.totalSpent.divide(BigDecimal.valueOf(this.orderCount), MONEY_SCALE, MONEY_ROUNDING);
    }

    private static BigDecimal nullToZero(BigDecimal v) {
        return v == null ? BigDecimal.ZERO : v;
    }

    private static BigDecimal money(BigDecimal v) {
        return v.setScale(MONEY_SCALE, MONEY_ROUNDING);
    }
}
