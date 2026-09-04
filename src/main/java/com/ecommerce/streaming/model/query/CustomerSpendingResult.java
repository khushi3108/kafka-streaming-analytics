package com.ecommerce.streaming.model.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Response body of {@code GET /api/analytics/customer-spending/{customerId}} — a KEYED lookup.
 *
 * <p>A keyed lookup needs no fan-out: {@code KafkaStreams.queryMetadataForKey} names the single
 * instance whose partition owns this key, so the request is either served locally or forwarded
 * to exactly that one host. That is the difference between an O(1) RPC and an O(instances) scan,
 * and it is why the keyed and unkeyed paths are implemented separately.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class CustomerSpendingResult {

    private String customerId;

    /** False when the key's owning instance has no record for it. Not an error, and not 404. */
    private boolean found;

    private BigDecimal totalSpent;
    private long orderCount;
    private BigDecimal avgOrderValue;
    /** EVENT time (epoch millis) of the most recent order. */
    private long lastOrderTimestamp;

    /** {@code host:port} of the instance whose partition actually owns this key. */
    private String servedBy;

    /** True when {@link #servedBy} is the instance that received the client request. */
    private boolean servedLocally;
}
