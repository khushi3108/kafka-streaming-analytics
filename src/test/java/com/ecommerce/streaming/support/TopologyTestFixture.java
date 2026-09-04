package com.ecommerce.streaming.support;

import com.ecommerce.streaming.config.OrderTimestampExtractor;
import com.ecommerce.streaming.model.CategorySales;
import com.ecommerce.streaming.model.EnrichedOrder;
import com.ecommerce.streaming.model.Order;
import com.ecommerce.streaming.model.OrderAlert;
import com.ecommerce.streaming.model.Product;
import com.ecommerce.streaming.serde.JsonSerde;
import com.ecommerce.streaming.streams.OrderStreamTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Shared harness for every {@link TopologyTestDriver} test in this package.
 *
 * <p>It builds the REAL {@link OrderStreamTopology} — not a re-implementation of it — wires the
 * same {@code OrderTimestampExtractor} the production config installs, and exposes the four
 * topics plus the state stores. Three previous rounds of verification were done with throwaway
 * drivers that were deleted afterwards; this class is what makes those checks repeatable.
 *
 * <h2>Determinism</h2>
 * <ul>
 *   <li>Every timestamp is explicit and derived from {@link #BASE}. Nothing sleeps, and no test
 *       reads the wall clock.</li>
 *   <li>The record cache is set to 0 bytes so the topology forwards on every update. That makes
 *       the number of emitted records an assertable fact instead of a caching artefact — which
 *       is exactly what the "one alert per window, not one per overlapping window" and
 *       "one final result per window" assertions need.</li>
 *   <li>The state directory is a fresh temp dir per fixture, so RocksDB never contends with a
 *       parallel test or a leftover run.</li>
 * </ul>
 *
 * <h2>Advancing stream time</h2>
 * Kafka Streams' clock is EVENT time: it only moves when a record with a later timestamp
 * arrives. {@link #advanceStreamTimeTo(Instant)} pipes one throwaway "filler" order to move it.
 * Each filler uses a unique customer id and its own category so it can never accumulate into a
 * customer signal (velocity needs &gt;3 orders, baseline needs &ge;5 prior) or pollute the
 * category under test.
 */
public final class TopologyTestFixture implements AutoCloseable {

    /** Anchor for every timestamp in the tests. Exactly on a minute boundary, so window maths is obvious. */
    public static final Instant BASE = Instant.parse("2024-01-01T00:00:00Z");

    public static final String ORDERS_TOPIC          = "orders";
    public static final String PRODUCTS_TOPIC        = "products";
    public static final String ENRICHED_ORDERS_TOPIC = "enriched-orders";
    public static final String FRAUD_ALERTS_TOPIC    = "fraud-alerts";
    public static final String CATEGORY_SALES_TOPIC  = "category-sales";

    /** Matches {@code app.fraud.threshold} in application.yml. */
    public static final BigDecimal FRAUD_THRESHOLD = new BigDecimal("500.0");

    private static final String FILLER_CATEGORY = "ZZ-Filler";

    private final TopologyTestDriver driver;
    private final TestInputTopic<String, Order> orders;
    private final TestInputTopic<String, Product> products;
    private final TestOutputTopic<String, EnrichedOrder> enrichedOrders;
    private final TestOutputTopic<String, OrderAlert> fraudAlerts;
    private final TestOutputTopic<String, CategorySales> categorySales;

    private final AtomicInteger fillerCounter = new AtomicInteger();

    public TopologyTestFixture() {
        OrderStreamTopology topology = new OrderStreamTopology();

        // The production class takes its configuration from @Value fields. Setting them
        // directly keeps the test on the real object instead of a subclass or a copy.
        ReflectionTestUtils.setField(topology, "ordersTopic", ORDERS_TOPIC);
        ReflectionTestUtils.setField(topology, "productsTopic", PRODUCTS_TOPIC);
        ReflectionTestUtils.setField(topology, "enrichedOrdersTopic", ENRICHED_ORDERS_TOPIC);
        ReflectionTestUtils.setField(topology, "fraudAlertsTopic", FRAUD_ALERTS_TOPIC);
        ReflectionTestUtils.setField(topology, "categorySalesTopic", CATEGORY_SALES_TOPIC);
        ReflectionTestUtils.setField(topology, "fraudThreshold", FRAUD_THRESHOLD);
        ReflectionTestUtils.setField(topology, "velocityMaxOrders", 3L);
        ReflectionTestUtils.setField(topology, "sessionMaxOrders", 8L);
        ReflectionTestUtils.setField(topology, "sessionMaxValue", new BigDecimal("3000.00"));
        ReflectionTestUtils.setField(topology, "baselineMultiplier", new BigDecimal("3.0"));
        ReflectionTestUtils.setField(topology, "baselineMinOrders", 5L);

        StreamsBuilder builder = new StreamsBuilder();
        topology.buildTopology(builder);

        this.driver = new TopologyTestDriver(builder.build(), streamsProperties());

        JsonSerde<Order> orderSerde = new JsonSerde<>(Order.class);
        JsonSerde<Product> productSerde = new JsonSerde<>(Product.class);
        JsonSerde<EnrichedOrder> enrichedSerde = new JsonSerde<>(EnrichedOrder.class);
        JsonSerde<OrderAlert> alertSerde = new JsonSerde<>(OrderAlert.class);
        JsonSerde<CategorySales> categorySalesSerde = new JsonSerde<>(CategorySales.class);

        this.orders = driver.createInputTopic(
                ORDERS_TOPIC, Serdes.String().serializer(), orderSerde.serializer());
        this.products = driver.createInputTopic(
                PRODUCTS_TOPIC, Serdes.String().serializer(), productSerde.serializer());
        this.enrichedOrders = driver.createOutputTopic(
                ENRICHED_ORDERS_TOPIC, Serdes.String().deserializer(), enrichedSerde.deserializer());
        this.fraudAlerts = driver.createOutputTopic(
                FRAUD_ALERTS_TOPIC, Serdes.String().deserializer(), alertSerde.deserializer());
        this.categorySales = driver.createOutputTopic(
                CATEGORY_SALES_TOPIC, Serdes.String().deserializer(), categorySalesSerde.deserializer());
    }

    private static Properties streamsProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // THE point of the event-time tests: the same extractor production installs, reading the
        // payload's `timestamp` field instead of the broker's append time.
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, OrderTimestampExtractor.class);

        // No caching: every update is forwarded, so emitted-record COUNTS are meaningful.
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
        props.put(StreamsConfig.STATE_DIR_CONFIG, tempStateDir());
        return props;
    }

    private static String tempStateDir() {
        try {
            Path dir = Files.createTempDirectory("kafka-streams-test-");
            dir.toFile().deleteOnExit();
            return dir.toAbsolutePath().toString();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot create a temporary Kafka Streams state directory", e);
        }
    }

    // ── Input helpers ───────────────────────────────────────────────────

    /** Publish a product to the compacted catalog topic that backs the enrichment join. */
    public void sendProduct(Product product) {
        // Products carry no event time; the extractor falls back to the record timestamp, so a
        // positive one is supplied rather than letting it reach for the wall clock.
        products.pipeInput(product.getProductId(), product, BASE);
    }

    /**
     * Pipe an order whose RECORD (broker) timestamp equals its payload event time — the normal
     * case, where the distinction does not show up.
     */
    public void sendOrder(Order order) {
        sendOrder(order, Instant.ofEpochMilli(order.getTimestamp()));
    }

    /**
     * Pipe an order whose RECORD timestamp is deliberately different from its payload
     * {@code timestamp}. This is how the event-time tests prove the extractor is in charge.
     *
     * @param recordTimestamp the broker/producer append time to stamp on the record
     */
    public void sendOrder(Order order, Instant recordTimestamp) {
        orders.pipeInput(order.getOrderId(), order, recordTimestamp);
    }

    /**
     * Move Kafka Streams' event-time clock to {@code target} by piping one throwaway order.
     *
     * <p>There is no other way: stream time is derived from record timestamps, and
     * {@code TopologyTestDriver.advanceWallClockTime} moves the punctuation clock, not the
     * event clock the windows use.
     */
    public void advanceStreamTimeTo(Instant target) {
        int n = fillerCounter.incrementAndGet();
        Order filler = Order.builder()
                .orderId("FILLER-" + n)
                // Unique customer per filler: it can never reach the >3 velocity threshold or the
                // 5-prior-order baseline threshold, so it cannot manufacture an alert.
                .customerId("FILLER-CUST-" + n)
                .productId("FILLER-PROD")
                .category(FILLER_CATEGORY)
                .amount(new BigDecimal("1.00"))
                .quantity(1)
                .status("CONFIRMED")
                .timestamp(target.toEpochMilli())
                .build();
        sendOrder(filler);
    }

    // ── Output helpers ──────────────────────────────────────────────────
    //
    // TestOutputTopic reads are DESTRUCTIVE — a second read returns only what arrived since
    // the first. That makes assertions order-dependent in a way that silently passes
    // (`assertThat(readAlerts("HIGH_VALUE")).isEmpty()` is vacuously true after any earlier
    // read). These accessors drain into a cumulative buffer instead, so every call returns
    // everything emitted so far and the order of assertions in a test cannot change its meaning.

    private final List<EnrichedOrder> enrichedSeen = new java.util.ArrayList<>();
    private final List<OrderAlert> alertsSeen = new java.util.ArrayList<>();
    private final List<KeyValue<String, CategorySales>> categorySalesSeen = new java.util.ArrayList<>();

    /** Every enriched order emitted so far. */
    public List<EnrichedOrder> readEnrichedOrders() {
        enrichedSeen.addAll(enrichedOrders.readValuesToList());
        return List.copyOf(enrichedSeen);
    }

    /** Every alert emitted so far. Filler orders can never raise one, by construction. */
    public List<OrderAlert> readAlerts() {
        alertsSeen.addAll(fraudAlerts.readValuesToList());
        return List.copyOf(alertsSeen);
    }

    /** Alerts of one signal only — the four signals share the {@code fraud-alerts} topic. */
    public List<OrderAlert> readAlerts(String alertType) {
        return readAlerts().stream()
                .filter(a -> alertType.equals(a.getAlertType()))
                .collect(Collectors.toList());
    }

    /** Category-sales output records emitted so far, with the filler category dropped. */
    public List<KeyValue<String, CategorySales>> readCategorySales() {
        categorySales.readKeyValuesToList().stream()
                .filter(kv -> !FILLER_CATEGORY.equals(kv.key))
                .forEach(categorySalesSeen::add);
        return List.copyOf(categorySalesSeen);
    }

    // ── State stores ────────────────────────────────────────────────────

    public KeyValueStore<String, com.ecommerce.streaming.model.CustomerSpending> customerSpendingStore() {
        return driver.getKeyValueStore(OrderStreamTopology.CUSTOMER_SPENDING_STORE);
    }

    public TopologyTestDriver driver() {
        return driver;
    }

    // ── Model builders ──────────────────────────────────────────────────

    /** An order that is below every threshold unless the caller raises the amount. */
    public static Order order(String orderId, String customerId, String category,
                              String amount, Instant eventTime) {
        return Order.builder()
                .orderId(orderId)
                .customerId(customerId)
                .productId("P001")
                .category(category)
                .amount(new BigDecimal(amount))
                .quantity(1)
                .status("CONFIRMED")
                .timestamp(eventTime.toEpochMilli())
                .build();
    }

    public static Instant at(long seconds) {
        return BASE.plus(Duration.ofSeconds(seconds));
    }

    @Override
    public void close() {
        driver.close();
    }
}
