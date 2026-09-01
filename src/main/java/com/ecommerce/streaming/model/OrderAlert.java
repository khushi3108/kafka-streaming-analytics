package com.ecommerce.streaming.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Fraud / anomaly alert generated when an order exceeds the configured threshold.
 * Published to the "fraud-alerts" Kafka topic.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderAlert {

    private String alertId;
    private String orderId;
    private String customerId;
    private String productId;
    private String productName;
    private String category;
    /** Order amount in USD – BigDecimal to stay consistent with Order/EnrichedOrder. */
    private BigDecimal amount;
    private int quantity;

    /**
     * Alert type:
     *  HIGH_VALUE_ORDER  – single order exceeds fraud threshold
     */
    private String alertType;

    /**
     * Severity based on amount:
     *  CRITICAL (> $2000), HIGH (> $1000), MEDIUM (> $500)
     */
    private String severity;
    private long timestamp;

    private static final BigDecimal SEVERITY_CRITICAL = new BigDecimal("2000");
    private static final BigDecimal SEVERITY_HIGH     = new BigDecimal("1000");

    /**
     * Namespace prefix for the name-based alert id, so an alert id can never collide with an
     * id derived from some other kind of entity that happens to share an orderId string.
     */
    private static final String ALERT_ID_NAMESPACE = "urn:ecommerce:order-alert:";

    /**
     * Build an alert from an EnrichedOrder.
     *
     * <p><b>Determinism.</b> This used to stamp {@code UUID.randomUUID()} and
     * {@code System.currentTimeMillis()} onto every alert. Both are non-deterministic, so a
     * redelivered order produced a byte-different second alert: downstream had no way to tell
     * "the same fraud alert, seen twice" from "two genuinely distinct alerts". ksqlDB's
     * {@code fraud_by_category COUNT(*)} double-counted it, and in a real deployment it pages
     * an on-call human a second time for one incident.
     *
     * <p>Both fields are now derived purely from the input record, so replaying the same order
     * yields a byte-identical alert and any consumer can dedupe on {@code alertId} (or use it as
     * an idempotency key when writing to a downstream store):
     * <ul>
     *   <li>{@code alertId} — a name-based UUID over (orderId, alertType, severity). Same order,
     *       same alert type ⇒ same id, on any JVM, at any time.</li>
     *   <li>{@code timestamp} — the ORDER's event time, carried through from the source record,
     *       not the wall clock at the moment the alert happened to be built.</li>
     * </ul>
     *
     * <p>Note this is exactly the property that makes exactly-once processing observable: EOS
     * prevents duplicate alerts on the happy path, and deterministic ids let a consumer detect
     * and drop them on the paths EOS does not cover (manual replay, reprocessing from an
     * earlier offset, the non-transactional dead-letter path).
     */
    public static OrderAlert fromEnrichedOrder(EnrichedOrder order, String alertType) {
        BigDecimal amount = order.getAmount() != null ? order.getAmount() : BigDecimal.ZERO;

        String severity;
        if (amount.compareTo(SEVERITY_CRITICAL) > 0) severity = "CRITICAL";
        else if (amount.compareTo(SEVERITY_HIGH) > 0) severity = "HIGH";
        else severity = "MEDIUM";

        return OrderAlert.builder()
                .alertId(deterministicAlertId(order.getOrderId(), alertType, severity))
                .orderId(order.getOrderId())
                .customerId(order.getCustomerId())
                .productId(order.getProductId())
                .productName(order.getProductName())
                .category(order.getCategory())
                .amount(amount)
                .quantity(order.getQuantity())
                .alertType(alertType)
                .severity(severity)
                // Event time of the order, NOT wall-clock time. A replayed order must produce a
                // byte-identical alert, and this is also the same clock the topology windows on
                // (OrderTimestampExtractor) and the clock ksqlDB uses (TIMESTAMP='timestamp').
                .timestamp(order.getTimestamp())
                .build();
    }

    /**
     * Stable, name-based alert id: a UUID derived by hashing the alert's identity rather than
     * drawing from a random source.
     *
     * <p>{@link UUID#nameUUIDFromBytes} produces a version-3 (MD5) name-based UUID. It is used
     * here purely as a deterministic identity function — it carries no security property and
     * MD5's collision weakness is irrelevant for that purpose. It is chosen over hand-rolling
     * SHA-1/v5 because it is in the JDK, needs no dependency, and formats as a normal UUID
     * string so nothing downstream has to change how it parses the field.
     *
     * <p>Public so that alerts built outside this class can be made replay-stable the same way.
     * Aggregate (windowed) alerts should pass something that identifies the <em>window</em>, not
     * just the customer — e.g. {@code customerId + "@" + window.start()} — otherwise every
     * window for a customer collapses onto one id.
     *
     * @param identity  what this alert is about: an orderId for per-order alerts, or a
     *                  customer+window key for aggregate alerts. Must be derived only from the
     *                  input records, never from a clock or a random source.
     * @param alertType the signal that fired
     * @param severity  the computed severity, included so a re-classified alert gets a new id
     */
    public static String deterministicAlertId(String identity, String alertType, String severity) {
        String name = ALERT_ID_NAMESPACE + identity + "|" + alertType + "|" + severity;
        return UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8)).toString();
    }
}

