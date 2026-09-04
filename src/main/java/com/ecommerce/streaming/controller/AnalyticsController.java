package com.ecommerce.streaming.controller;

import com.ecommerce.streaming.model.query.CategorySalesResponse;
import com.ecommerce.streaming.model.query.CustomerSpendingResponse;
import com.ecommerce.streaming.model.query.CustomerSpendingResult;
import com.ecommerce.streaming.model.query.StreamsStatusResponse;
import com.ecommerce.streaming.service.InteractiveQueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;

/**
 * ═══════════════════════════════════════════════════════════════════════════
 *  REST API over the Kafka Streams Interactive Query state stores.
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * <p>Interactive Queries read the state Kafka Streams already maintains, with no second
 * datastore in the path. The catch — and it is a big one — is that state is <b>sharded by
 * partition across instances</b>: an instance can only read the partitions it was assigned.
 * All the cross-instance work (discovery, RPC fan-out, merging, partial-result reporting) lives
 * in {@link InteractiveQueryService}; this class is a thin HTTP layer over it.
 *
 * <h2>Endpoints</h2>
 * <pre>
 *   GET /api/analytics/category-sales?from=&amp;to=&amp;windowMinutes=&amp;local=
 *   GET /api/analytics/customer-spending?local=
 *   GET /api/analytics/customer-spending/{customerId}?local=
 *   GET /api/analytics/streams/status
 * </pre>
 *
 * <h2>The {@code local} parameter</h2>
 * {@code local=true} makes an instance answer from ITS OWN partitions only, skipping discovery
 * and RPC. It is how instances query each other, and it is the reason the fan-out terminates:
 * without it, every instance would fan out to every other instance, forever. It is also useful
 * by hand when debugging which shard holds what.
 *
 * <h2>Partial results</h2>
 * Fan-out responses carry {@code partial} and {@code failures}. {@code partial=true} means the
 * body is a SUBSET of the real answer because some instance could not be reached. Callers must
 * branch on it — the whole point of the change is that an incomplete answer is no longer
 * indistinguishable from a complete one.
 *
 * <h2>Status codes</h2>
 * {@code 503 + Retry-After} for rebalances and restoring stores (normal and retryable),
 * {@code 400} for bad parameters, {@code 404} for unknown resources. See
 * {@code ApiExceptionHandler}; nothing here throws a bare {@code IllegalStateException} into
 * Spring's default 500 handler any more.
 */
@Slf4j
@RestController
@RequestMapping("/api/analytics")
@RequiredArgsConstructor
public class AnalyticsController {

    private final InteractiveQueryService interactiveQueries;

    /** Guardrail on the implicit range so a typo cannot ask for a decade of windows. */
    private static final long MAX_WINDOW_MINUTES = 60 * 24 * 7;

    // ── Category Sales (windowed store) ─────────────────────────────────

    /**
     * Windowed per-category sales, one entry per (category, window).
     *
     * <h3>Windows are no longer collapsed</h3>
     * The response is a LIST keyed by {@code (category, windowStart, windowEnd)}. The previous
     * implementation returned a {@code Map<String, CategorySales>} keyed by category and
     * resolved collisions with
     * {@code (existing, incoming) -> incoming.getOrderCount() > existing.getOrderCount() ? incoming : existing}.
     * Windows are 1 minute; the default query span is 2 minutes; so for every category active in
     * both windows the endpoint reported ONE window and silently discarded the other. Distinct
     * windows are distinct facts and are now returned as such. Where instances have to be
     * combined, the merge SUMS (see {@code CategorySales.merge}) — it never picks a winner.
     *
     * <h3>The range is EVENT time, and it is explicit</h3>
     * The store is keyed by the ORDER's own {@code timestamp} field (see
     * {@code OrderTimestampExtractor}), not by when the record was processed. The old code built
     * its range from {@code Instant.now()} — wall clock — and queried an event-time store with
     * it. The two agree only while the app is caught up and live. They do not agree after a
     * restart: {@code auto.offset.reset=earliest} replays the topic from the beginning, so the
     * store fills with windows whose event times are minutes or hours in the PAST, while the
     * query keeps asking for "the last 2 minutes of wall clock". The endpoint returns an empty
     * list and the app looks dead when it is in fact working perfectly.
     *
     * <p>So: pass {@code from} and {@code to} explicitly (ISO-8601 instants) whenever you care.
     * {@code windowMinutes} remains as a convenience that still anchors on {@code now}, and its
     * wall-clock assumption is now documented rather than hidden. To see everything the store
     * holds regardless of event time, pass {@code from=1970-01-01T00:00:00Z}.
     *
     * @param from          inclusive EVENT-time lower bound (window start times are matched
     *                      against this range). Defaults to {@code to - windowMinutes}.
     * @param to            inclusive EVENT-time upper bound. Defaults to now (wall clock).
     * @param windowMinutes convenience span used only when {@code from} is absent.
     * @param local         true = this instance's partitions only, no fan-out.
     */
    @GetMapping("/category-sales")
    public ResponseEntity<CategorySalesResponse> getCategorySales(
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(defaultValue = "2") long windowMinutes,
            @RequestParam(defaultValue = "false") boolean local) {

        if (windowMinutes <= 0 || windowMinutes > MAX_WINDOW_MINUTES) {
            throw new IllegalArgumentException(
                    "windowMinutes must be between 1 and " + MAX_WINDOW_MINUTES + ", got " + windowMinutes);
        }

        Instant rangeEnd = (to != null) ? to : Instant.now();
        Instant rangeStart = (from != null) ? from : rangeEnd.minus(Duration.ofMinutes(windowMinutes));

        if (rangeStart.isAfter(rangeEnd)) {
            throw new IllegalArgumentException(
                    "'from' (" + rangeStart + ") must not be after 'to' (" + rangeEnd + ")");
        }

        return ResponseEntity.ok(interactiveQueries.categorySales(rangeStart, rangeEnd, local));
    }

    // ── Customer Spending (key-value store) ─────────────────────────────

    /**
     * Lifetime running spend for EVERY customer, ranked by total spent descending.
     *
     * <p>This is the endpoint the silent-partial-results bug hit hardest. {@code store.all()}
     * iterates only the instance's own partitions, so with two instances this returned about
     * half the customers with HTTP 200. It now fans out to every instance in parallel and merges;
     * if any instance cannot answer, {@code partial=true} and {@code failures} say which.
     *
     * <p>Note this store is <b>not</b> a session-window store, whatever the old javadoc said.
     * It is an unwindowed {@code KTable} — a customer's LIFETIME running total, which is exactly
     * what the BASELINE_DEVIATION fraud signal needs as a baseline. The genuinely time-scoped
     * views live in {@code customer-velocity-store} (hopping) and {@code customer-session-store}
     * (session).
     *
     * @param local true = this instance's partitions only, no fan-out.
     */
    @GetMapping("/customer-spending")
    public ResponseEntity<CustomerSpendingResponse> getAllCustomerSpending(
            @RequestParam(defaultValue = "false") boolean local) {
        return ResponseEntity.ok(interactiveQueries.allCustomerSpending(local));
    }

    /**
     * Lifetime running spend for ONE customer, served from wherever that customer's partition
     * lives. A keyed lookup, so this is a single targeted hop rather than a fan-out.
     *
     * <p>An unknown customer is {@code 200} with {@code found:false}, not {@code 404}: the key
     * is perfectly valid, that customer simply has not ordered yet.
     *
     * @param local true = read this instance's store only. Answers {@code found:false} if the
     *              key belongs to a different instance, so only use it for debugging.
     */
    @GetMapping("/customer-spending/{customerId}")
    public ResponseEntity<CustomerSpendingResult> getCustomerSpending(
            @PathVariable String customerId,
            @RequestParam(defaultValue = "false") boolean local) {
        return ResponseEntity.ok(interactiveQueries.customerSpending(customerId, local));
    }

    // ── Streams status / instance discovery ─────────────────────────────

    /**
     * Kafka Streams lifecycle state plus every instance discovered through the group metadata,
     * with the stores and partitions each one owns.
     *
     * <p>Always {@code 200}, including while REBALANCING — this is the endpoint you call to find
     * out why the query endpoints are returning 503, so it must not 503 itself.
     */
    @GetMapping("/streams/status")
    public ResponseEntity<StreamsStatusResponse> streamsStatus() {
        return ResponseEntity.ok(interactiveQueries.status());
    }
}
