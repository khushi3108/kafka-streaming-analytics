package com.ecommerce.streaming.streams;

import com.ecommerce.streaming.model.*;
import com.ecommerce.streaming.serde.JsonSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
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
 *  3. Re-key enriched orders by orderId
 *  4. BRANCH A: filter amount > threshold  →  fraud-alerts
 *  5. BRANCH B: group by category + tumbling window 1 min (+30s grace),
 *               suppressed until the window closes
 *               aggregate(count, sum, avg)  →  category-sales-store
 *  6. BRANCH C: group by customerId, UNBOUNDED running aggregate
 *               aggregate(total spent)      →  customer-spending-store
 *
 *  Kafka Topics (OUTPUT):
 *    enriched-orders   – every order with product name/brand
 *    fraud-alerts      – orders exceeding the fraud threshold
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

    @Value("${app.fraud.threshold:500.0}")
    private BigDecimal fraudThreshold;

    // ── State store names (used by AnalyticsController for IQ) ──────────
    public static final String CATEGORY_SALES_STORE   = "category-sales-store";
    public static final String CUSTOMER_SPENDING_STORE = "customer-spending-store";
    public static final String PRODUCTS_STORE          = "products-store";

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

    /**
     * Spring Kafka calls this method automatically because it is annotated with
     * @Autowired and accepts a StreamsBuilder. The topology is registered before
     * KafkaStreams.start() is called.
     */
    @Autowired
    public void buildTopology(StreamsBuilder builder) {

        // ── Serdes ──────────────────────────────────────────────────────
        JsonSerde<Order>         orderSerde         = new JsonSerde<>(Order.class);
        JsonSerde<Product>       productSerde       = new JsonSerde<>(Product.class);
        JsonSerde<EnrichedOrder> enrichedOrderSerde = new JsonSerde<>(EnrichedOrder.class);
        JsonSerde<OrderAlert>    alertSerde         = new JsonSerde<>(OrderAlert.class);
        JsonSerde<CategorySales> categorySalesSerde = new JsonSerde<>(CategorySales.class);

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
        //  STEP 5 – FRAUD / HIGH-VALUE ORDER DETECTION
        //           Filter orders whose amount exceeds the configured
        //           threshold (default $500) and publish to fraud-alerts.
        // ════════════════════════════════════════════════════════════════
        enrichedStream
                .filter((orderId, order) ->
                                order.getAmount() != null
                                        && order.getAmount().compareTo(fraudThreshold) > 0,
                        Named.as("fraud-filter"))
                .peek((k, v) -> log.warn("🚨 FRAUD ALERT – orderId={} amount=${} customer={}",
                        v.getOrderId(), v.getAmount(), v.getCustomerId()))
                .mapValues(order -> OrderAlert.fromEnrichedOrder(order, "HIGH_VALUE_ORDER"),
                        Named.as("build-alert"))
                .to(fraudAlertsTopic,
                        Produced.with(Serdes.String(), alertSerde)
                                .withName("fraud-alerts-sink"));

        // ════════════════════════════════════════════════════════════════
        //  STEP 6 – CATEGORY SALES AGGREGATION
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
        // ═══════════════════════════════════════════════════════���════════
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
        //  STEP 7 – CUSTOMER SPENDING TRACKER (UNBOUNDED running total)
        //
        //  NOTE: this is NOT a session window. It is a plain `aggregate` with no
        //  `windowedBy(...)`, i.e. a lifetime running total per customerId that is
        //  never expired. (The previous comment here claimed "Session Window –
        //  5 minutes" and "session gaps > 5 min close the window"; no such code
        //  existed. Corrected rather than implemented — see the TODO below.)
        //
        //  Materialised as a KeyValueStore for Interactive Queries.
        //
        //  TODO(A4): this store grows without bound — one entry per customerId that
        //  is retained forever, plus an ever-growing changelog topic. Converting it
        //  to a SessionWindows aggregation (5-minute inactivity gap) is task A4.
        //
        //  TODO(money): the running total is still a `double` (Serdes.Double), so it
        //  carries the same compounding float error the category aggregate just shed.
        //  Migrating it to BigDecimal changes the state-store value type, which the
        //  AnalyticsController reads via Interactive Queries — deliberately left to
        //  the agent that owns that controller.
        // ════════════════════════════════════════════════════════════════
        enrichedStream
                .groupBy(
                        (orderId, order) -> order.getCustomerId(),
                        Grouped.<String, EnrichedOrder>as("grouped-by-customer")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(enrichedOrderSerde)
                )
                .aggregate(
                        () -> 0.0,
                        (customerId, order, totalSpent) -> order.getAmount() == null
                                ? totalSpent
                                : BigDecimal.valueOf(totalSpent)
                                        .add(order.getAmount())
                                        .doubleValue(),
                        Materialized
                                .<String, Double, KeyValueStore<Bytes, byte[]>>as(CUSTOMER_SPENDING_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double())
                );

        log.info("✅ Kafka Streams topology built successfully");
    }
}

