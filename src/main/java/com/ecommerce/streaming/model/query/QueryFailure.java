package com.ecommerce.streaming.model.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * One instance that could NOT be included in a fan-out Interactive Query result.
 *
 * <p>This type exists so that a partial answer can say so. The previous controller read only
 * the state stores local to whichever instance served the request: with two instances each
 * owning half the partitions, {@code /customer-spending} returned half the customers with
 * HTTP 200 and no indication that anything was missing. A silently wrong answer is worse than
 * an error, because nothing downstream can detect it.
 *
 * @see com.ecommerce.streaming.service.InteractiveQueryService
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryFailure {

    /** {@code host:port} of the instance that could not be queried. */
    private String host;

    /**
     * Why it could not be queried, e.g. {@code REBALANCING}, {@code STORE_NOT_AVAILABLE},
     * {@code RPC_FAILED}, {@code NO_APPLICATION_SERVER}.
     */
    private String reason;

    /** Exception message / HTTP status detail, for humans reading the response. */
    private String detail;
}
