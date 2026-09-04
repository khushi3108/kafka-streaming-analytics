package com.ecommerce.streaming.streams;

import com.ecommerce.streaming.model.*;
import com.ecommerce.streaming.serde.JsonSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;

/**
 * ═══════════════════════════════════════════════════════════════════════
 *   E-Commerce Order Stream Topology
 * ═══════════════════════════════════════════════════════════════════════
 *
 *  Kafka Topics (INPUT):
 *    ┌──────────┐         ┌──────────┐
 *    │  orders  │         │ products │  ← compacted KTable
 *    └────┬─────┘         └────┬─────┘
 *         │                   │
 *  Topology Steps:
 *
 *  1. Re-key orders by productId
 *  2. LEFT JOIN orders ← products  →  EnrichedOrder
 *  3. Re-key enriched orders by orderId  →  enriched-orders
 *  4. BRANCH A: group by category + tumbling window 1 min (+30s grace),
 *               suppressed until the window closes
 *               aggregate(count, sum, avg)  →  category-sales-store
 *  5. BRANCH B: re-key by customerId (one shared repartition), then four
 *               independent anomaly signals, all merged into fraud-alerts:
 *                 HIGH_VALUE          – single order over the absolute threshold
 *                 VELOCITY            – too many orders in a hopping 5m/1m window
 *                 SESSION_BURST       – a whole session (5m inactivity gap) is abnormal
 *                 BASELINE_DEVIATION  – order >> this customer's own running average
 *               plus the lifetime running total  →  customer-spending-store
 *
 *  Kafka Topics (OUTPUT):
 *    enriched-orders   – every order with product name/brand
 *    fraud-alerts      – multi-signal anomaly alerts (alertType says which signal fired)
 *    category-sales    – windowed per-category aggregations
 */
@Slf4j
@Component
public class OrderStreamTopology {

    // ── Topic names (from application.yml) ──────────────────────────────
    @Value("${app.topics.orders}")
    private String ordersTopic;

    @Value("${app.topics.products}")
    private String productsTopic;

    @Value("${app.topics.enriched-orders}")
    private String enrichedOrdersTopic;

    @Value("${app.topics.fraud-alerts}")
    private String fraudAlertsTopic;

    @Value("${app.topics.category-sales}")
    private String categorySalesTopic;

    // ── Detection thresholds ────────────────────────────────────────────
    // All defaulted inline so the topology runs unchanged against the current
    // application.yml; each can be overridden later without touching this class.

    /** SIGNAL 1 – absolute high-value screen. One signal among four, no longer "the system". */
    @Value("${app.fraud.threshold:500.0}")
    private BigDecimal fraudThreshold;

    /** SIGNAL 2 – alert when a customer exceeds this many orders inside the velocity window. */
    @Value("${app.fraud.velocity.max-orders:3}")
    private long velocityMaxOrders;

    /** SIGNAL 3 – a session with at least this many orders is a burst. */
    @Value("${app.fraud.session.max-orders:8}")
    private long sessionMaxOrders;

    /** SIGNAL 3 – ...or a session spending more than this, however few orders it took. */
    @Value("${app.fraud.session.max-value:3000.00}")
    private BigDecimal sessionMaxValue;

    /** SIGNAL 4 – flag an order worth more than N x the customer's own historical average. */
    @Value("${app.fraud.baseline.multiplier:3.0}")
    private BigDecimal baselineMultiplier;

    /**
     * SIGNAL 4 – minimum number of PRIOR orders before the baseline is trusted.
     * Without this, a customer's very first order is trivially "3x the average of nothing"
     * and every new customer would be flagged.
     */
    @Value("${app.fraud.baseline.min-orders:5}")
    private long baselineMinOrders;

    // ── State store names (used by AnalyticsController for IQ) ──────────
    public static final String CATEGORY_SALES_STORE    = "category-sales-store";
    /** Unchanged name — Interactive Queries keep working; only the VALUE TYPE changed. */
    public static final String CUSTOMER_SPENDING_STORE = "customer-spending-store";
    public static final String PRODUCTS_STORE          = "products-store";
    /** Windowed store behind the VELOCITY signal (hopping window). */
    public static final String CUSTOMER_VELOCITY_STORE = "customer-velocity-store";
    /** Session store behind the SESSION_BURST signal. */
    public static final String CUSTOMER_SESSION_STORE  = "customer-session-store";

    // ── Windowing ───────────────────────────────────────────────────────
    /** Tumbling window size for the per-category sales aggregation. */
    private static final Duration WINDOW_SIZE  = Duration.ofMinutes(1);
    /**
     * How long after a window's end we still accept records that belong to it.
     * With zero grace, any order whose EVENT time landed in an already-closed window
     * was dropped from the aggregate – yet the same record still reached
     * `enriched-orders`, still raised a fraud alert and still incremented customer
     * spending, leaving the three outputs mutually inconsistent.
     */
    private static final Duration WINDOW_GRACE = Duration.ofSeconds(30);

    /** VELOCITY: how much history each evaluation looks back over. */
    private static final Duration VELOCITY_WINDOW  = Duration.ofMinutes(5);
    /** VELOCITY: how often a new window opens. Smaller = finer-grained, more windows to keep. */
    private static final Duration VELOCITY_ADVANCE = Duration.ofMinutes(1);
    private static final Duration VELOCITY_GRACE   = Duration.ofMinutes(1);

    /** SESSION_BURST: a customer's session ends after this much inactivity. */
    private static final Duration SESSION_INACTIVITY_GAP = Duration.ofMinutes(5);
    private static final Duration SESSION_GRACE          = Duration.ofMinutes(1);

    private static final int MONEY_SCALE = 2;
    private static final RoundingMode MONEY_ROUNDING = RoundingMode.HALF_UP;

    /**
     * Spring Kafka calls this method automatically because it is annotated with
     * @Autowired and accepts a StreamsBuilder. The topology is registered before
     * KafkaStreams.start() is called.
     */
    @Autowired
    public void buildTopology(StreamsBuilder builder) {

        // ── Serdes ──────────────────────────────────────────────────────
        JsonSerde<Order>            orderSerde            = new JsonSerde<>(Order.class);
        JsonSerde<Product>          productSerde          = new JsonSerde<>(Product.class);
        JsonSerde<EnrichedOrder>    enrichedOrderSerde    = new JsonSerde<>(EnrichedOrder.class);
        JsonSerde<OrderAlert>       alertSerde            = new JsonSerde<>(OrderAlert.class);
        JsonSerde<CategorySales>    categorySalesSerde    = new JsonSerde<>(CategorySales.class);
        JsonSerde<CustomerSpending> customerSpendingSerde = new JsonSerde<>(CustomerSpending.class);

        // ════════════════════════════════════════════════════════════════
        //  STEP 1 – Source: Read from "orders" topic
        //           Key: orderId  |  Value: Order JSON
        // ════════════════════════════════════════════════════════════════
        KStream<String, Order> ordersStream = builder.stream(
                ordersTopic,
                Consumed.with(Serdes.String(), orderSerde)
                        .withName("orders-source")
        );

        // ════════════════════════════════════════════════════════════════
        //  STEP 2 – Source: Load "products" topic as a KTable
        //           Key: productId  |  Value: Product JSON
        //           The compacted topic ensures we always have the latest product info.
        // ════════════════════════════════════════════════════════════════
        KTable<String, Product> productsTable = builder.table(
                productsTopic,
                Consumed.with(Serdes.String(), productSerde),
                Materialized.<String, Product, KeyValueStore<Bytes, byte[]>>as(PRODUCTS_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(productSerde)
        );

        // ════════════════════════════════════════════════════════════════
        //  STEP 3 – Re-key the orders stream by productId so we can join
        //           with the products KTable (which is keyed by productId).
        //           selectKey() marks the stream for repartitioning.
        // ════════════════════════════════════════════════════════════════
        KStream<String, Order> ordersByProduct = ordersStream
                .selectKey((orderId, order) -> order.getProductId(),
                        Named.as("rekey-by-productId"));

        // ════════════════════════════════════════════════════════════════
        //  STEP 4 – KStream–KTable LEFT JOIN
        //           For each order, look up the corresponding product.
        //           LEFT JOIN: if the product is not yet in the table,
        //           product argument is null → handled in EnrichedOrder.from()
        // ════════════════════════════════════════════════════════════════
        KStream<String, EnrichedOrder> enrichedByProduct = ordersByProduct.leftJoin(
                productsTable,
                EnrichedOrder::from,                        // ValueJoiner
                Joined.with(Serdes.String(), orderSerde, productSerde)
                      .withName("orders-products-join")
        );

        // Re-key back to orderId so downstream consumers can look up by orderId
        KStream<String, EnrichedOrder> enrichedStream = enrichedByProduct
                .selectKey((productId, enriched) -> enriched.getOrderId(),
                        Named.as("rekey-by-orderId"));

        // Publish enriched orders
        enrichedStream.to(enrichedOrdersTopic,
                Produced.with(Serdes.String(), enrichedOrderSerde)
                        .withName("enriched-orders-sink"));

        // ════════════════════════════════════════════════════════════════
        //  STEP 5 – CATEGORY SALES AGGREGATION
        //           Tumbling window: 1 minute size + 30 second grace,
        //           suppressed so each window emits a single final result.
        //
        //  SQL equivalent (ksqlDB):
        //    SELECT category, COUNT(*), SUM(amount), AVG(amount)
        //    FROM enriched_orders
        //    WINDOW TUMBLING (SIZE 1 MINUTE)
        //    GROUP BY category EMIT CHANGES;
        //
        //  The result is materialised in "category-sales-store" for
        //  Interactive Queries (AnalyticsController).
        // ════════════════════════════════════════════════════════════════
        KTable<Windowed<String>, CategorySales> categorySalesTable = enrichedStream
                .groupBy(
                        (orderId, order) -> order.getCategory(),
                        Grouped.<String, EnrichedOrder>as("grouped-by-category")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(enrichedOrderSerde)
                )
                .windowedBy(
                        // 1-minute tumbling window with a 30s grace period, so a record
                        // that arrives slightly late still lands in the window its EVENT
                        // time says it belongs to instead of being silently dropped.
                        TimeWindows.ofSizeAndGrace(WINDOW_SIZE, WINDOW_GRACE)
                )
                .aggregate(
                        CategorySales::new,                   // initializer
                        (category, order, agg) ->             // aggregator
                                agg.update(category, order.getAmount(), order.getQuantity()),
                        Materialized
                                .<String, CategorySales, WindowStore<Bytes, byte[]>>as(CATEGORY_SALES_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(categorySalesSerde)
                );

        categorySalesTable
                // Emit ONE final record per window instead of an update per input record.
                // The buffer is unbounded: it holds a window's aggregate in memory until
                // window end + grace has passed, then forwards the final value downstream.
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream(Named.as("category-sales-stream"))
                .map((windowedKey, sales) -> {
                    // Bug fix: SLF4J uses {} placeholders, not printf-style ${:.2f}
                    log.info("📊 Category [{}] window=[{} .. {}] orders={} totalSales={}",
                            windowedKey.key(),
                            windowedKey.window().startTime(), windowedKey.window().endTime(),
                            sales.getOrderCount(), sales.getTotalSales());
                    return new org.apache.kafka.streams.KeyValue<>(windowedKey.key(), sales);
                })
                .to(categorySalesTopic,
                        Produced.with(Serdes.String(), categorySalesSerde)
                                .withName("category-sales-sink"));

        // ════════════════════════════════════════════════════════════════
        //  STEP 6 – RE-KEY BY customerId (once, explicitly)
        //
        //  Everything customer-scoped below — the spending store and all three
        //  stateful anomaly signals — must be keyed by customerId. The stream
        //  arrives keyed by orderId, so a repartition is unavoidable: Kafka Streams
        //  can only aggregate or join per key if every record for that key lands on
        //  the same partition, and therefore the same task.
        //
        //  We do the selectKey + repartition ONCE, explicitly, instead of letting
        //  four separate groupBy(customerId) calls each mark the stream dirty. That
        //  buys two things:
        //    1. ONE internal repartition topic (`...-orders-by-customer-repartition`)
        //       instead of four copies of the same data.
        //    2. CO-PARTITIONING for free. The KStream→KTable join in the
        //       BASELINE_DEVIATION signal below joins this stream against the
        //       customer-spending KTable; both derive from this same repartition
        //       topic, so they have identical keys and identical partition counts —
        //       the co-partitioning requirement Streams enforces at startup.
        //  Note the internal repartition topic is created and managed by Streams,
        //  so KafkaTopicConfig needs no new bean.
        // ════════════════════════════════════════════════════════════════
        KStream<String, EnrichedOrder> ordersByCustomer = enrichedStream
                .selectKey((orderId, order) -> order.getCustomerId(),
                        Named.as("rekey-by-customerId"))
                .repartition(
                        Repartitioned.<String, EnrichedOrder>as("orders-by-customer")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(enrichedOrderSerde)
                );

        // ════════════════════════════════════════════════════════════════
        //  STEP 7 – CUSTOMER SPENDING TRACKER (lifetime running total)
        //
        //  Materialised as a KeyValueStore under the SAME store name as before
        //  ("customer-spending-store") so AnalyticsController's Interactive Queries
        //  keep resolving — but the value type is now CustomerSpending instead of
        //  Double. The old `BigDecimal.valueOf(total).add(amount).doubleValue()`
        //  workaround re-introduced float error on every single record; money now
        //  stays BigDecimal end-to-end.
        //
        //  This is deliberately NOT windowed: it is the customer's lifetime baseline,
        //  which is exactly what the BASELINE_DEVIATION signal needs. Bounded,
        //  time-scoped views of the same activity live in the velocity (hopping) and
        //  session stores below.
        // ════════════════════════════════════════════════════════════════
        KTable<String, CustomerSpending> customerSpendingTable = ordersByCustomer
                // Already keyed by customerId and already repartitioned, so groupByKey()
                // adds no second shuffle.
                .groupByKey(
                        Grouped.<String, EnrichedOrder>as("grouped-by-customer")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(enrichedOrderSerde)
                )
                .aggregate(
                        CustomerSpending::new,
                        (customerId, order, agg) ->
                                agg.update(customerId, order.getAmount(), order.getTimestamp()),
                        Materialized
                                .<String, CustomerSpending, KeyValueStore<Bytes, byte[]>>as(CUSTOMER_SPENDING_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(customerSpendingSerde)
                );

        // ════════════════════════════════════════════════════════════════
        //  STEP 8 – ANOMALY DETECTION (four independent signals)
        //
        //  The previous implementation was a single `filter(amount > 500)`. A constant
        //  comparison has no notion of the customer, of rate, or of what is normal —
        //  it flags one legitimate laptop purchase and misses twenty $499 card tests.
        //  Each signal below answers a different question; they are merged into one
        //  `fraud-alerts` stream and `alertType` records which one fired.
        // ════════════════════════════════════════════════════════════════

        // ── SIGNAL 1: HIGH_VALUE ────────────────────────────────────────
        // Kept, but demoted: a single order above the absolute threshold. Still useful
        // as a floor (a $50k order is worth looking at regardless of history), just no
        // longer the whole of "fraud detection".
        KStream<String, OrderAlert> highValueAlerts = ordersByCustomer
                .filter((customerId, order) ->
                                order.getAmount() != null
                                        && order.getAmount().compareTo(fraudThreshold) > 0,
                        Named.as("high-value-filter"))
                .peek((k, v) -> log.warn("🚨 HIGH_VALUE – orderId={} amount={} customer={}",
                        v.getOrderId(), v.getAmount(), v.getCustomerId()))
                .mapValues(order -> OrderAlert.fromEnrichedOrder(order, "HIGH_VALUE"),
                        Named.as("build-high-value-alert"));

        // ── SIGNAL 2: VELOCITY (hopping window) ─────────────────────────
        // "More than N orders from one customer in the last 5 minutes."
        // A HOPPING window (5 min size, advancing every 1 min) rather than a tumbling
        // one because a tumbling window resets on a fixed boundary: 3 orders at 11:59
        // and 3 more at 12:01 would never be seen together. Hopping windows overlap, so
        // every minute there is a window covering the trailing five.
        //
        // Cost of that: each record belongs to WINDOW/ADVANCE = 5 windows simultaneously,
        // so a naive filter would emit the same burst five times. The `isTrailingWindow`
        // guard below keeps only the OLDEST window containing the record — the one that
        // actually spans the preceding five minutes of history — collapsing the fan-out
        // to a single evaluation per record.
        KStream<String, OrderAlert> velocityAlerts = ordersByCustomer
                .groupByKey(
                        Grouped.<String, EnrichedOrder>as("grouped-by-customer-velocity")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(enrichedOrderSerde)
                )
                .windowedBy(TimeWindows.ofSizeAndGrace(VELOCITY_WINDOW, VELOCITY_GRACE)
                        .advanceBy(VELOCITY_ADVANCE))
                .aggregate(
                        CustomerSpending::new,
                        (customerId, order, agg) ->
                                agg.update(customerId, order.getAmount(), order.getTimestamp()),
                        Materialized
                                .<String, CustomerSpending, WindowStore<Bytes, byte[]>>as(CUSTOMER_VELOCITY_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(customerSpendingSerde)
                )
                // NOT suppressed: a velocity alert that arrives 6 minutes after the burst is
                // useless. We accept emitting on every update instead, which means a sustained
                // burst re-alerts as it grows — desirable here, and bounded by the record cache
                // + commit interval rather than by every single input record.
                .toStream(Named.as("velocity-stream"))
                .filter((windowedKey, spending) ->
                                spending != null
                                        && spending.getOrderCount() > velocityMaxOrders
                                        && isTrailingWindow(windowedKey, spending.getLastOrderTimestamp()),
                        Named.as("velocity-filter"))
                .map((windowedKey, spending) -> {
                    log.warn("🚨 VELOCITY – customer={} orders={} total={} window=[{} .. {}]",
                            windowedKey.key(), spending.getOrderCount(), spending.getTotalSpent(),
                            windowedKey.window().startTime(), windowedKey.window().endTime());
                    return KeyValue.pair(windowedKey.key(),
                            aggregateAlert(windowedKey, spending, "VELOCITY", "HIGH"));
                }, Named.as("build-velocity-alert"));

        // ── SIGNAL 3: SESSION_BURST (session window) ────────────────────
        // Session windows group a customer's activity by ACTIVITY, not by the clock: a
        // session runs until the customer goes quiet for 5 minutes, however long that
        // takes. That is the natural unit for "was this shopping session as a whole
        // abnormal?", which is a question no fixed-size window can ask — the previous
        // code's comment claimed a 5-minute session window and never had one.
        //
        // Suppressed until the window closes, unlike velocity: a session's verdict is
        // only meaningful once the session has actually ended, and without suppression
        // every session merge emits a tombstone plus a new record.
        KStream<String, OrderAlert> sessionAlerts = ordersByCustomer
                .groupByKey(
                        Grouped.<String, EnrichedOrder>as("grouped-by-customer-session")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(enrichedOrderSerde)
                )
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(SESSION_INACTIVITY_GAP, SESSION_GRACE))
                .aggregate(
                        CustomerSpending::new,
                        (customerId, order, agg) ->
                                agg.update(customerId, order.getAmount(), order.getTimestamp()),
                        // The merger is what makes session windows different: when a late record
                        // bridges the gap between two sessions, Streams merges their aggregates.
                        // CustomerSpending.merge recomputes the average from the combined totals
                        // instead of averaging two averages.
                        (customerId, left, right) -> CustomerSpending.merge(left, right),
                        Materialized
                                .<String, CustomerSpending, SessionStore<Bytes, byte[]>>as(CUSTOMER_SESSION_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(customerSpendingSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream(Named.as("session-stream"))
                .filter((windowedKey, spending) ->
                                spending != null && isSessionBurst(spending),
                        Named.as("session-burst-filter"))
                .map((windowedKey, spending) -> {
                    String severity = spending.getTotalSpent().compareTo(sessionMaxValue) > 0
                            ? "CRITICAL" : "HIGH";
                    log.warn("🚨 SESSION_BURST – customer={} orders={} total={} session=[{} .. {}]",
                            windowedKey.key(), spending.getOrderCount(), spending.getTotalSpent(),
                            windowedKey.window().startTime(), windowedKey.window().endTime());
                    return KeyValue.pair(windowedKey.key(),
                            aggregateAlert(windowedKey, spending, "SESSION_BURST", severity));
                }, Named.as("build-session-alert"));

        // ── SIGNAL 4: BASELINE_DEVIATION (stream ⋈ table) ───────────────
        // "Is this order abnormal FOR THIS CUSTOMER?" — the question a fixed threshold
        // cannot ask. Every order is joined back against the customer's own running
        // aggregate and flagged when it exceeds N x their historical average.
        //
        // Co-partitioning: both sides come from the `orders-by-customer` repartition
        // above, so they share a key space and a partition count and land in the same
        // task — the join needs no further shuffle.
        //
        // Self-inclusion: `customerSpendingTable` is built from the SAME stream, and its
        // aggregate node was added to the topology BEFORE this join node. Kafka Streams
        // forwards a record to a node's children in the order they were added, depth
        // first, so by the time the join runs the store ALREADY contains the current
        // order. `baselineDeviationAlert` therefore backs the current order out of the
        // aggregate before comparing, giving a genuine prior-history baseline rather
        // than one the order under test has already inflated.
        KStream<String, OrderAlert> baselineAlerts = ordersByCustomer
                .leftJoin(
                        customerSpendingTable,
                        this::baselineDeviationAlert,
                        Joined.with(Serdes.String(), enrichedOrderSerde, customerSpendingSerde)
                                .withName("baseline-deviation-join")
                )
                .filter((customerId, alert) -> alert != null, Named.as("baseline-deviation-filter"));

        // ── Merge every signal into the single fraud-alerts topic ───────
        // Reusing the existing topic (rather than adding per-signal topics) keeps
        // KafkaTopicConfig and the ksqlDB `fraud_alerts_stream` schema untouched;
        // consumers discriminate on `alertType`.
        //
        // All four streams are keyed by customerId, so every alert about one customer
        // lands on one partition in order — which is what an alert consumer wants when
        // it correlates signals. (The high-value branch used to be keyed by orderId.)
        highValueAlerts
                .merge(baselineAlerts, Named.as("merge-baseline-alerts"))
                .merge(velocityAlerts, Named.as("merge-velocity-alerts"))
                .merge(sessionAlerts, Named.as("merge-session-alerts"))
                .to(fraudAlertsTopic,
                        Produced.with(Serdes.String(), alertSerde)
                                .withName("fraud-alerts-sink"));

        log.info("✅ Kafka Streams topology built successfully");
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Detection helpers
    // ═══════════════════════════════════════════════════════════════════

    /**
     * A hopping window advances every {@link #VELOCITY_ADVANCE}, so a record at time
     * {@code t} falls into {@code WINDOW/ADVANCE} windows at once. Only one of them
     * spans the five minutes BEFORE {@code t}: the oldest, starting at
     * {@code floor(t / advance) * advance - (size - advance)}. Every newer window
     * mostly covers the future and has barely any history in it yet.
     *
     * <p>Restricting alerts to that window means one evaluation per record instead of
     * five near-identical alerts for the same burst.
     */
    private static boolean isTrailingWindow(Windowed<String> windowedKey, long eventTimestamp) {
        if (eventTimestamp <= 0) return false;
        long advanceMs = VELOCITY_ADVANCE.toMillis();
        long trailingStart = (eventTimestamp / advanceMs) * advanceMs
                - (VELOCITY_WINDOW.toMillis() - advanceMs);
        return windowedKey.window().start() == trailingStart;
    }

    /** A session is a burst if it holds too many orders OR too much money. */
    private boolean isSessionBurst(CustomerSpending spending) {
        return spending.getOrderCount() >= sessionMaxOrders
                || (spending.getTotalSpent() != null
                        && spending.getTotalSpent().compareTo(sessionMaxValue) > 0);
    }

    /**
     * BASELINE_DEVIATION joiner. Returns {@code null} (filtered out downstream) when the
     * order looks normal for this customer, or when there is not enough history to judge.
     *
     * @param order    the order being evaluated
     * @param spending this customer's running aggregate, already including {@code order}
     *                 (see the join comment in {@code buildTopology})
     */
    private OrderAlert baselineDeviationAlert(EnrichedOrder order, CustomerSpending spending) {
        if (order == null || order.getAmount() == null
                || spending == null || spending.getTotalSpent() == null) {
            return null;
        }

        // Back the current order out of the aggregate to recover the prior baseline.
        long priorOrderCount = spending.getOrderCount() - 1;
        if (priorOrderCount < baselineMinOrders) {
            // Not enough history: a first (or nearly first) order must not be flagged just
            // for existing. This is the guard that stops "3x the average" from being
            // trivially true for every new customer.
            return null;
        }

        BigDecimal priorTotal = spending.getTotalSpent().subtract(order.getAmount());
        if (priorTotal.signum() <= 0) return null;

        BigDecimal priorAvg = priorTotal.divide(
                BigDecimal.valueOf(priorOrderCount), MONEY_SCALE, MONEY_ROUNDING);
        if (priorAvg.signum() <= 0) return null;

        BigDecimal ceiling = priorAvg.multiply(baselineMultiplier);
        if (order.getAmount().compareTo(ceiling) <= 0) return null;

        log.warn("🚨 BASELINE_DEVIATION – customer={} orderId={} amount={} priorAvg={} priorOrders={}",
                order.getCustomerId(), order.getOrderId(), order.getAmount(), priorAvg, priorOrderCount);
        return OrderAlert.fromEnrichedOrder(order, "BASELINE_DEVIATION");
    }

    /**
     * Builds an alert for the two AGGREGATE signals (velocity, session), which describe a
     * group of orders rather than one order.
     *
     * <p>{@code orderId}/{@code productId}/{@code category} are intentionally left null:
     * the alert is about the customer's behaviour over a window, not about any single
     * order, and inventing a representative orderId would be misleading. {@code amount}
     * carries the window's TOTAL spend, and {@code reason} carries the order count behind it —
     * the fact that actually justifies the alert and previously had nowhere to go.
     *
     * <p><b>Determinism (the carry-over fix).</b> This helper still stamped
     * {@code UUID.randomUUID()} and {@code System.currentTimeMillis()} long after
     * {@link OrderAlert#fromEnrichedOrder} had been made replay-stable, so two of the four
     * signals quietly kept the old behaviour: replay the same burst and you got a second alert
     * with a different id and a different timestamp, indistinguishable from a genuine new
     * incident. Both are now derived purely from the input:
     *
     * <ul>
     *   <li><b>identity includes the WINDOW.</b> {@code customerId + "@" + window.start()}, not
     *       the customerId alone. Keying on the customer alone would collapse every window that
     *       customer ever triggers onto ONE alert id — a consumer deduping on alertId would then
     *       drop every burst after the first, which is worse than the random ids it replaced.
     *       The window start is the thing that makes two bursts by the same customer two
     *       different events.</li>
     *   <li><b>timestamp is EVENT time</b> — the last order time folded into the window's
     *       aggregate. Same clock as the windowing, the same clock as ksqlDB, and stable across
     *       a replay.</li>
     * </ul>
     */
    private static OrderAlert aggregateAlert(Windowed<String> windowedKey,
                                             CustomerSpending spending,
                                             String alertType,
                                             String severity) {
        String customerId = windowedKey.key();
        long windowStart = windowedKey.window().start();

        // Identity = customer + window. Both come from the record stream, never from a clock.
        String identity = customerId + "@" + windowStart;

        // Plain concatenation rather than String.format: no default-Locale dependency, so the
        // text is byte-identical on any JVM (the alert as a whole is meant to be replay-stable).
        String reason = alertType + ": " + spending.getOrderCount() + " orders totalling "
                + (spending.getTotalSpent() == null ? "0" : spending.getTotalSpent().toPlainString())
                + " between " + windowedKey.window().startTime()
                + " and " + windowedKey.window().endTime();

        return OrderAlert.builder()
                .alertId(OrderAlert.deterministicAlertId(identity, alertType, severity))
                .customerId(customerId)
                .amount(spending.getTotalSpent())
                .alertType(alertType)
                .severity(severity)
                .reason(reason)
                // EVENT time of the last order in the window, NOT the wall clock.
                .timestamp(spending.getLastOrderTimestamp())
                .build();
    }
}
