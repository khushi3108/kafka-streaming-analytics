package com.ecommerce.streaming.model.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Response body of {@code GET /api/analytics/customer-spending} (the unkeyed scan).
 *
 * <p>This is THE endpoint the silent-partial-results bug bit hardest: {@code store.all()}
 * returns only the partitions local to the instance serving the request, so with two
 * instances it answered HTTP 200 with roughly half the customers and no way to tell.
 * {@link #partial} and {@link #failures} exist so that can never happen quietly again.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class CustomerSpendingResponse {

    private String fetchedAt;

    /** TRUE when at least one instance was unreachable — {@code customers} is then a SUBSET. */
    private boolean partial;

    /** Every {@code host:port} the coordinator attempted, including itself. */
    private List<String> hostsQueried;

    private List<QueryFailure> failures;

    /** True when this body is a single instance's local partitions only (the RPC leg). */
    private boolean localOnly;

    private int totalCustomers;

    /** Sorted by {@code totalSpent} descending. */
    private List<CustomerSpendingEntry> customers;
}
