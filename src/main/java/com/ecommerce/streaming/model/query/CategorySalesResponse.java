package com.ecommerce.streaming.model.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Response body of {@code GET /api/analytics/category-sales}.
 *
 * <p>Also the wire format of the internal RPC leg: a remote instance answers the same shape
 * with {@code local=true}, and the coordinating instance deserializes it back into this type.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class CategorySalesResponse {

    /** Inclusive EVENT-time lower bound actually used for the scan, ISO-8601 UTC. */
    private String from;
    /** Inclusive EVENT-time upper bound actually used for the scan, ISO-8601 UTC. */
    private String to;
    private long fromMs;
    private long toMs;

    /** Wall-clock time the response was produced. Never used to build the scan range. */
    private String fetchedAt;

    /**
     * TRUE when at least one instance owning part of the store could not be reached or was
     * not able to serve. The {@code windows} list is then a SUBSET of the real answer.
     * Clients must treat a partial result as incomplete rather than as "the data".
     */
    private boolean partial;

    /** Every {@code host:port} the coordinator attempted, including itself. */
    private List<String> hostsQueried;

    /** One entry per instance that was dropped from the result. Empty when {@code partial=false}. */
    private List<QueryFailure> failures;

    /** True when this body is a single instance's local partitions only (the RPC leg). */
    private boolean localOnly;

    private int windowCount;

    /** One entry per (category, window). Distinct windows stay distinct. */
    private List<CategorySalesWindow> windows;
}
