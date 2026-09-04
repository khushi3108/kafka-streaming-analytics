package com.ecommerce.streaming.integration;

import com.ecommerce.streaming.config.OrderTimestampExtractor;
import com.ecommerce.streaming.model.CategorySales;
import com.ecommerce.streaming.model.CustomerSpending;
import com.ecommerce.streaming.model.Order;
import com.ecommerce.streaming.model.Product;
import com.ecommerce.streaming.serde.JsonSerde;
import com.ecommerce.streaming.streams.OrderStreamTopology;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.StoreQueryParameters;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ═══════════════════════════════════════════════════════════════════════════
 *  END-TO-END: the real topology, against a REAL Kafka broker in Docker.
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * <h2>How to run this</h2>
 * <pre>
 *   export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
 *   # Docker must be running:
 *   docker info
 *   ./gradlew integrationTest
 * </pre>
 *
 * <p>It is <b>excluded from {@code ./gradlew test}</b> via {@code @Tag("integration")}, and
 * additionally annotated {@code @Testcontainers(disabledWithoutDocker = true)} so that even a
 * direct run without a Docker daemon SKIPS rather than fails. The absence of Docker must never
 * turn into a red default build.
 *
 * <h2>What it proves that TopologyTestDriver cannot</h2>
 * {@code TopologyTestDriver} runs the topology in one thread with no broker: it cannot exercise
 * real serialization over the wire, real repartition and changelog topics, real
 * {@code EXACTLY_ONCE_V2} transactions, or the interaction between suppression and an actual
 * commit interval. This test drives a fixed corpus with a PRECOMPUTED total through all of that
 * and asserts the final aggregate is exactly right — not approximately right.
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class OrderStreamTopologyKafkaIT {

    private static final String ORDERS_TOPIC          = "orders";
    private static final String PRODUCTS_TOPIC        = "products";
    private static final String ENRICHED_ORDERS_TOPIC = "enriched-orders";
    private static final String FRAUD_ALERTS_TOPIC    = "fraud-alerts";
    private static final String CATEGORY_SALES_TOPIC  = "category-sales";

    private static final String CATEGORY = "Electronics";

    /** Anchor for the corpus. On a minute boundary, so the whole corpus falls in one window. */
    private static final Instant BASE = Instant.parse("2024-01-01T00:00:00Z");

    // ── THE CORPUS ──────────────────────────────────────────────────────
    // 25 orders of 19.99 and 25 orders of 0.07, all inside the first minute.
    //   25 x 19.99 = 499.75
    //   25 x  0.07 =   1.75
    //             total 501.50   ← precomputed by hand, not by the code under test
    // Both amounts are unrepresentable in binary floating point, so a double-based aggregation
    // lands a few cents away from this number and the assertion below fails.
    private static final int ORDERS_PER_AMOUNT = 25;
    private static final int CORPUS_SIZE = ORDERS_PER_AMOUNT * 2;
    private static final BigDecimal EXPECTED_TOTAL = new BigDecimal("501.50");
    private static final BigDecimal EXPECTED_AVG   = new BigDecimal("10.03"); // 501.50 / 50

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.3"));

    private static KafkaStreams streams;

    @BeforeAll
    static void startTopology() throws Exception {
        createTopics();
        seedProducts();
        produceCorpus();

        streams = new KafkaStreams(buildTopology(), streamsProperties());
        CountDownLatch running = new CountDownLatch(1);
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) running.countDown();
        });
        streams.start();
        assertThat(running.await(90, TimeUnit.SECONDS))
                .as("KafkaStreams did not reach RUNNING")
                .isTrue();
    }

    @AfterAll
    static void stopTopology() {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Tests
    // ═══════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("A 50-order corpus aggregates to EXACTLY the precomputed total, end to end")
    void windowedAggregateMatchesThePrecomputedTotalExactly() {
        JsonSerde<CategorySales> serde = new JsonSerde<>(CategorySales.class);

        List<CategorySales> emitted = consume(CATEGORY_SALES_TOPIC, Duration.ofSeconds(120),
                records -> records.stream()
                        .filter(r -> CATEGORY.equals(r.key()))
                        .map(r -> serde.deserializer().deserialize(CATEGORY_SALES_TOPIC, r.value()))
                        .toList(),
                found -> !found.isEmpty());

        assertThat(emitted)
                .as("suppress(untilWindowCloses) must emit ONE final record for the window")
                .hasSize(1);

        CategorySales sales = emitted.get(0);
        assertThat(sales.getOrderCount()).isEqualTo(CORPUS_SIZE);
        assertThat(sales.getTotalSales())
                .as("exact money survives real serialization, repartitioning and EOS commits")
                .isEqualByComparingTo(EXPECTED_TOTAL);
        assertThat(sales.getTotalSales().toPlainString()).isEqualTo("501.50");
        assertThat(sales.getAvgOrderValue()).isEqualByComparingTo(EXPECTED_AVG);
        assertThat(sales.getMinOrderValue()).isEqualByComparingTo(new BigDecimal("0.07"));
        assertThat(sales.getMaxOrderValue()).isEqualByComparingTo(new BigDecimal("19.99"));
    }

    @Test
    @DisplayName("Every order reaches enriched-orders, with the catalog join applied")
    void everyOrderIsEnriched() {
        JsonSerde<com.ecommerce.streaming.model.EnrichedOrder> serde =
                new JsonSerde<>(com.ecommerce.streaming.model.EnrichedOrder.class);

        List<com.ecommerce.streaming.model.EnrichedOrder> enriched = consume(
                ENRICHED_ORDERS_TOPIC, Duration.ofSeconds(120),
                records -> records.stream()
                        .map(r -> serde.deserializer().deserialize(ENRICHED_ORDERS_TOPIC, r.value()))
                        .toList(),
                // The closing order is part of the stream too, hence >=.
                found -> found.size() >= CORPUS_SIZE);

        assertThat(enriched).hasSizeGreaterThanOrEqualTo(CORPUS_SIZE);
        assertThat(enriched).filteredOn(e -> "P001".equals(e.getProductId()))
                .allSatisfy(e -> assertThat(e.getProductName()).isEqualTo("MacBook Pro 16\""));
    }

    @Test
    @DisplayName("The customer-spending store holds the exact lifetime total for a customer")
    void customerSpendingStoreIsExact() {
        // Every corpus order belongs to the same customer, so its lifetime total is the whole
        // corpus total. Read through a real Interactive Query against a real RocksDB store.
        ReadOnlyKeyValueStore<String, CustomerSpending> store = awaitStore();

        CustomerSpending spending = awaitValue(store, "C-CORPUS");
        assertThat(spending.getOrderCount()).isEqualTo(CORPUS_SIZE);
        assertThat(spending.getTotalSpent()).isEqualByComparingTo(EXPECTED_TOTAL);
        assertThat(spending.getAvgOrderValue()).isEqualByComparingTo(EXPECTED_AVG);
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Cluster / topology setup
    // ═══════════════════════════════════════════════════════════════════

    private static void createTopics() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (Admin admin = Admin.create(props)) {
            NewTopic products = new NewTopic(PRODUCTS_TOPIC, 1, (short) 1);
            products.configs(java.util.Map.of(TopicConfig.CLEANUP_POLICY_CONFIG,
                    TopicConfig.CLEANUP_POLICY_COMPACT));
            admin.createTopics(List.of(
                    new NewTopic(ORDERS_TOPIC, 1, (short) 1),
                    products,
                    new NewTopic(ENRICHED_ORDERS_TOPIC, 1, (short) 1),
                    new NewTopic(FRAUD_ALERTS_TOPIC, 1, (short) 1),
                    new NewTopic(CATEGORY_SALES_TOPIC, 1, (short) 1)
            )).all().get(60, TimeUnit.SECONDS);
        }
    }

    private static void seedProducts() {
        JsonSerde<Product> serde = new JsonSerde<>(Product.class);
        Product product = new Product("P001", "MacBook Pro 16\"", CATEGORY,
                new BigDecimal("2499.99"), "Apple", "M3 Pro chip, 18GB RAM");
        try (Producer<String, byte[]> producer = byteProducer()) {
            producer.send(new ProducerRecord<>(PRODUCTS_TOPIC, product.getProductId(),
                    serde.serializer().serialize(PRODUCTS_TOPIC, product)));
            producer.flush();
        }
    }

    private static void produceCorpus() {
        JsonSerde<Order> serde = new JsonSerde<>(Order.class);
        try (Producer<String, byte[]> producer = byteProducer()) {
            int i = 0;
            for (String amount : List.of("19.99", "0.07")) {
                for (int n = 0; n < ORDERS_PER_AMOUNT; n++, i++) {
                    Order order = Order.builder()
                            .orderId("O-" + i)
                            .customerId("C-CORPUS")
                            .productId("P001")
                            .category(CATEGORY)
                            .amount(new BigDecimal(amount))
                            .quantity(1)
                            .status("CONFIRMED")
                            // All inside [BASE, BASE+1m) — one tumbling window.
                            .timestamp(BASE.plusSeconds(i % 55).toEpochMilli())
                            .build();
                    producer.send(new ProducerRecord<>(ORDERS_TOPIC, order.getOrderId(),
                            serde.serializer().serialize(ORDERS_TOPIC, order)));
                }
            }

            // A single order far in the future, to push EVENT time past window end + grace so the
            // suppression buffer flushes. Different customer and category so it cannot contribute
            // to anything under assertion.
            Order closer = Order.builder()
                    .orderId("O-CLOSER").customerId("C-CLOSER").productId("P999")
                    .category("ZZ-Closer").amount(new BigDecimal("1.00")).quantity(1)
                    .status("CONFIRMED")
                    .timestamp(BASE.plus(Duration.ofMinutes(10)).toEpochMilli())
                    .build();
            producer.send(new ProducerRecord<>(ORDERS_TOPIC, closer.getOrderId(),
                    serde.serializer().serialize(ORDERS_TOPIC, closer)));
            producer.flush();
        }
    }

    private static Producer<String, byte[]> byteProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new KafkaProducer<>(props);
    }

    /** The REAL production topology class, configured the way Spring configures it. */
    private static org.apache.kafka.streams.Topology buildTopology() {
        OrderStreamTopology topology = new OrderStreamTopology();
        ReflectionTestUtils.setField(topology, "ordersTopic", ORDERS_TOPIC);
        ReflectionTestUtils.setField(topology, "productsTopic", PRODUCTS_TOPIC);
        ReflectionTestUtils.setField(topology, "enrichedOrdersTopic", ENRICHED_ORDERS_TOPIC);
        ReflectionTestUtils.setField(topology, "fraudAlertsTopic", FRAUD_ALERTS_TOPIC);
        ReflectionTestUtils.setField(topology, "categorySalesTopic", CATEGORY_SALES_TOPIC);
        ReflectionTestUtils.setField(topology, "fraudThreshold", new BigDecimal("500.0"));
        ReflectionTestUtils.setField(topology, "velocityMaxOrders", 3L);
        ReflectionTestUtils.setField(topology, "sessionMaxOrders", 8L);
        ReflectionTestUtils.setField(topology, "sessionMaxValue", new BigDecimal("3000.00"));
        ReflectionTestUtils.setField(topology, "baselineMultiplier", new BigDecimal("3.0"));
        ReflectionTestUtils.setField(topology, "baselineMinOrders", 5L);

        StreamsBuilder builder = new StreamsBuilder();
        topology.buildTopology(builder);
        return builder.build();
    }

    private static Properties streamsProperties() throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "it-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, OrderTimestampExtractor.class);

        // The production guarantee, exercised for real.
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // ONE broker in the container, so the production RF=3 / min.isr=2 for internal topics
        // cannot be satisfied here. Replication is a cluster property, not a topology property,
        // and it is what docker-compose.yml's three-broker setup covers.
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.topicPrefix(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG), "1");

        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.STATE_DIR_CONFIG,
                Files.createTempDirectory("kafka-streams-it-").toAbsolutePath().toString());
        return props;
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Polling helpers — deadline-bounded, never a bare sleep
    // ═══════════════════════════════════════════════════════════════════

    private <T> List<T> consume(String topic,
                                Duration timeout,
                                java.util.function.Function<List<ConsumerRecord<String, byte[]>>, List<T>> mapper,
                                java.util.function.Predicate<List<T>> done) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // The topology writes under EXACTLY_ONCE_V2; an uncommitted read could see records from a
        // transaction that is later aborted.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        List<ConsumerRecord<String, byte[]>> collected = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, byte[]> polled = consumer.poll(Duration.ofMillis(500));
                polled.forEach(collected::add);
                List<T> mapped = mapper.apply(collected);
                if (done.test(mapped)) {
                    return mapped;
                }
            }
            return mapper.apply(collected);
        }
    }

    private ReadOnlyKeyValueStore<String, CustomerSpending> awaitStore() {
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        RuntimeException last = null;
        while (System.nanoTime() < deadline) {
            try {
                return streams.store(StoreQueryParameters.fromNameAndType(
                        OrderStreamTopology.CUSTOMER_SPENDING_STORE,
                        QueryableStoreTypes.keyValueStore()));
            } catch (RuntimeException e) {
                last = e; // still restoring / rebalancing
            }
        }
        throw new AssertionError("customer-spending-store never became queryable", last);
    }

    private CustomerSpending awaitValue(ReadOnlyKeyValueStore<String, CustomerSpending> store,
                                        String key) {
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        CustomerSpending value = null;
        while (System.nanoTime() < deadline) {
            value = store.get(key);
            if (value != null && value.getOrderCount() == CORPUS_SIZE) {
                return value;
            }
        }
        throw new AssertionError("customer-spending-store never reached " + CORPUS_SIZE
                + " orders for " + key + " (last seen: " + value + ")");
    }
}
