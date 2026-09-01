# Code Explanation
## E-Commerce Kafka Streams + ksqlDB — Detailed Technical Walkthrough

---

## 1. Project Structure and Responsibilities

```
config/
  KafkaTopicConfig.java       → Spring-managed topic creation (AdminClient)
  KafkaStreamsConfig.java      → Kafka Streams bootstrap + ALL StreamsConfig props

model/
  Order.java                  → Input event (published to orders topic)
  Product.java                → Catalog entry (published to products topic, KTable source)
  EnrichedOrder.java          → Output of KStream-KTable join
  OrderAlert.java             → Output of fraud filter
  CategorySales.java          → Mutable aggregation accumulator

serde/
  JsonSerde.java              → Generic Jackson-based Kafka Serde<T>

producer/
  OrderProducer.java          → Publishes Order JSON to orders topic
  ProductProducer.java        → @PostConstruct: publishes product catalog on startup

streams/
  OrderStreamTopology.java    → THE MAIN FILE: complete Kafka Streams topology

service/
  DataSimulator.java          → Generates realistic random orders for demo

controller/
  OrderController.java        → REST: produce orders, control simulation
  AnalyticsController.java    → REST: query Kafka Streams Interactive State Stores
```

---

## 2. `application.yml` — Configuration

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092   # all Kafka clients connect here

    producer:
      key-serializer:   StringSerializer  # orderId as String key
      value-serializer: StringSerializer  # JSON string value (serialised by us)

    admin:
      fail-fast: false  # if Kafka is briefly unavailable at boot, log warning, don't crash

    streams:
      application-id: ecommerce-streams-app   # consumer group prefix + state dir name
      bootstrap-servers: localhost:9092
      # ⚠ ALL other StreamsConfig is set in KafkaStreamsConfig.java to avoid
      # duplicate/conflicting configuration between YAML and Java bean.

app:
  topics:
    orders: orders              # used by @Value in OrderStreamTopology
    products: products
    enriched-orders: enriched-orders
    fraud-alerts: fraud-alerts
    category-sales: category-sales
  fraud:
    threshold: 500.0            # orders above this amount → fraud-alerts topic
```

**Design decision: why not put everything in YAML?**  
When you define a `@Bean(name = DEFAULT_STREAMS_CONFIG_BEAN_NAME)` in Java, Spring Kafka uses that bean for all Streams configuration and ignores most `spring.kafka.streams.*` YAML properties. Mixing both creates silent overrides and subtle bugs. The pattern used here: minimal YAML (just `application-id` and `bootstrap-servers`) + all `StreamsConfig` props in the Java bean.

---

## 3. `KafkaStreamsConfig.java` — Enabling and Configuring Streams

```java
@Configuration
@EnableKafkaStreams   // ← activates StreamsBuilderFactoryBean lifecycle management
public class KafkaStreamsConfig {

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.streams.application-id}") String applicationId) {
```

**`@EnableKafkaStreams` does three things:**
1. Creates a `StreamsBuilderFactoryBean` (manages `KafkaStreams` lifecycle — start/stop with Spring context)
2. Exposes a `StreamsBuilder` bean that topology classes can `@Autowired`
3. Registers the `KafkaStreams` instance so `StreamsBuilderFactoryBean.getKafkaStreams()` works in controllers

**`@Value` on method parameters (not fields):**  
A subtle but important safety pattern. In `@Configuration` classes, `@Bean` methods are called as part of Spring's bean initialisation phase. If `@Value` were on class fields, there's no guarantee the fields are populated before the `@Bean` method runs. Method parameter injection is resolved before the method executes — guaranteed.

**Key `StreamsConfig` properties set:**

| Property | Value | Why |
|----------|-------|-----|
| `CACHE_MAX_BYTES_BUFFERING_CONFIG` | `0` | Disables record cache — updates appear immediately in state stores. (In production, set to 10MB+ for throughput) |
| `NUM_STREAM_THREADS_CONFIG` | `2` | Two threads process partitions in parallel |
| `AUTO_OFFSET_RESET_CONFIG` | `earliest` | On first run, process all existing records from the beginning |
| `DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER` | `LogAndContinueExceptionHandler` | Skip corrupted records and log warning instead of crashing |
| `STATE_DIR_CONFIG` | `/tmp/kafka-streams/ecommerce` | Where RocksDB state stores are persisted on disk |

---

## 4. `JsonSerde.java` — Generic Kafka Serializer/Deserializer

Kafka's built-in `Serdes` class provides `String`, `Integer`, `Double`, `ByteArray` etc., but nothing for POJOs. This project requires serializing `Order`, `Product`, `EnrichedOrder`, `OrderAlert`, and `CategorySales` to/from JSON.

```java
public class JsonSerde<T> implements Serde<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final Class<T> targetType;

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> MAPPER.writeValueAsBytes(data);
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> MAPPER.readValue(bytes, targetType);
    }
}
```

**Usage in the topology:**
```java
JsonSerde<Order> orderSerde = new JsonSerde<>(Order.class);
KStream<String, Order> ordersStream = builder.stream(
    "orders",
    Consumed.with(Serdes.String(), orderSerde)   // key=String, value=Order
);
```

One `JsonSerde<T>` instance per type, created at topology build time (not per-record). The `ObjectMapper` is static and shared — it is thread-safe when configured at construction time.

---

## 5. `OrderStreamTopology.java` — The Core of the System

This is the most important file. It uses the Kafka Streams DSL to declare a topology that processes orders in real time. The `@Autowired void buildTopology(StreamsBuilder builder)` method is called by Spring before `KafkaStreams.start()`, registering all nodes in the topology.

### Step 1 & 2: Source Streams

```java
// Source 1: orders arrive as a continuous, unbounded stream
KStream<String, Order> ordersStream = builder.stream(
    ordersTopic,
    Consumed.with(Serdes.String(), new JsonSerde<>(Order.class))
);

// Source 2: products loaded as a KTable — latest value per productId
// The compacted topic means Kafka only keeps the most recent product per key
KTable<String, Product> productsTable = builder.table(
    productsTopic,
    Consumed.with(Serdes.String(), new JsonSerde<>(Product.class)),
    Materialized.as(PRODUCTS_STORE)    // named state store → queryable via IQ
);
```

`builder.stream()` creates a `KStream` — an infinite sequence of records.  
`builder.table()` creates a `KTable` — a materialised view of the latest value per key, backed by a local RocksDB store.

### Step 3 & 4: KStream–KTable Join

```java
// Problem: orders are keyed by orderId, but the products KTable is keyed by productId
// For the join to work, both sides must share the same key.
// selectKey() re-keys the stream and marks it for automatic repartition.
KStream<String, Order> ordersByProduct = ordersStream
    .selectKey((orderId, order) -> order.getProductId());

// Left join: look up product details for each order
// If productId not found in table (product not yet loaded), product = null
KStream<String, EnrichedOrder> enrichedByProduct = ordersByProduct.leftJoin(
    productsTable,
    EnrichedOrder::from,                        // ValueJoiner: (Order, Product) → EnrichedOrder
    Joined.with(Serdes.String(), orderSerde, productSerde)
);

// Re-key back to orderId for all downstream processing
KStream<String, EnrichedOrder> enrichedStream = enrichedByProduct
    .selectKey((productId, enriched) -> enriched.getOrderId());
```

**Why `leftJoin` and not `join`?**  
The `products` topic is populated at startup, but there's a race condition — if an order arrives before the product KTable is fully loaded, a strict `join` would drop that order record. `leftJoin` passes `null` for the product argument instead, and `EnrichedOrder.from()` handles it gracefully (`productName = "Unknown Product"`).

### Step 5: Fraud Detection Filter

```java
enrichedStream
    .filter((orderId, order) -> order.getAmount() > fraudThreshold)  // stateless O(1)
    .peek((k, v) -> log.warn("🚨 FRAUD ALERT orderId={} amount={}", v.getOrderId(), v.getAmount()))
    .mapValues(order -> OrderAlert.fromEnrichedOrder(order, "HIGH_VALUE_ORDER"))
    .to(fraudAlertsTopic, Produced.with(Serdes.String(), alertSerde));
```

`filter()` is a **stateless** operation — it requires no memory of past events, processes each record independently in O(1). This is the simplest form of stream processing and executes with the lowest possible latency.

`peek()` does not transform the stream — it is purely a side-effect for logging. It does not affect the topology.

### Step 6: Windowed Category Aggregation

```java
enrichedStream
    .groupBy(
        (k, order) -> order.getCategory(),         // group key: category string
        Grouped.with(Serdes.String(), enrichedOrderSerde)
    )
    .windowedBy(
        TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))  // 1-minute tumbling window
    )
    .aggregate(
        CategorySales::new,                         // () → fresh CategorySales accumulator
        (category, order, accumulator) ->           // called for EVERY record in the window
            accumulator.update(category, order.getAmount(), order.getQuantity()),
        Materialized
            .<String, CategorySales, WindowStore<Bytes, byte[]>>as(CATEGORY_SALES_STORE)
            .withValueSerde(categorySalesSerde)     // how to persist CategorySales in RocksDB
    )
    .toStream()
    .map((windowedKey, sales) -> new KeyValue<>(windowedKey.key(), sales))
    .to(categorySalesTopic, Produced.with(Serdes.String(), categorySalesSerde));
```

**How the window aggregation works:**
1. `groupBy(category)` re-keys the stream by category, triggering a repartition
2. `windowedBy(TimeWindows.ofSizeWithNoGrace(...))` assigns each record to a time window based on its `timestamp` field
3. For each (category, window) combination, the `aggregate` initialiser creates a new `CategorySales` object
4. The `aggregator` function is called once per record — it updates the accumulator in-place
5. The accumulated `CategorySales` is stored in the `category-sales-store` RocksDB state store
6. When queried via Interactive Queries (REST), the store returns the current state

`ofSizeWithNoGrace()` means the window accepts no late-arriving records (for simplicity in a demo).

### Step 7: Customer Spending Tracker

```java
enrichedStream
    .groupBy((k, order) -> order.getCustomerId())
    .aggregate(
        () -> 0.0,                                  // initialiser: start at $0
        (customerId, order, total) -> total + order.getAmount(),  // add each order
        Materialized.as(CUSTOMER_SPENDING_STORE)
            .withValueSerde(Serdes.Double())
    );
```

No windowing here — this is an **unbounded aggregation**. The running total grows for every order a customer places, forever. It is stored in `customer-spending-store` (RocksDB) and read by `AnalyticsController`.

---

## 6. `CategorySales.java` — The Aggregation Accumulator

```java
@Data
@NoArgsConstructor                      // ← Kafka Streams requires a no-arg constructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CategorySales {
    private String category;
    private long orderCount;
    private double totalSales;
    private int totalQuantity;
    private double avgOrderValue;
    private double maxOrderValue;
    private double minOrderValue = Double.MAX_VALUE;

    public CategorySales update(String category, double amount, int quantity) {
        this.category = category;
        this.orderCount++;
        this.totalSales += amount;
        this.totalQuantity += quantity;
        this.avgOrderValue = this.totalSales / this.orderCount;
        if (amount > this.maxOrderValue) this.maxOrderValue = amount;
        if (amount < this.minOrderValue) this.minOrderValue = amount;
        return this;          // ← must return 'this' — the aggregator lambda needs the updated value
    }
}
```

**Why mutable?** Kafka Streams' `aggregate()` passes the same object back to the aggregator for each new record in the window. The object is read from RocksDB (deserialised), updated in-place, then serialised back. Making it mutable avoids object allocation per record and is the standard Kafka Streams pattern.

**Why `@NoArgsConstructor`?** The initialiser lambda `CategorySales::new` calls the no-arg constructor to create a fresh accumulator for each new window. Without it, the lambda would not compile.

---

## 7. `AnalyticsController.java` — Interactive Queries

```java
@GetMapping("/category-sales")
public ResponseEntity<Map<String, Object>> getCategorySales(
        @RequestParam(defaultValue = "2") long windowMinutes) {

    // Get the running KafkaStreams instance via Spring's factory bean
    KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();

    // Access the named window store — NO database, NO network call
    // This reads directly from the local RocksDB instance in this JVM
    ReadOnlyWindowStore<String, CategorySales> store = streams.store(
        StoreQueryParameters.fromNameAndType(
            OrderStreamTopology.CATEGORY_SALES_STORE,
            QueryableStoreTypes.windowStore()
        )
    );

    // Fetch all category windows that started within the last N minutes
    Instant to = Instant.now();
    Instant from = to.minus(Duration.ofMinutes(windowMinutes));

    Map<String, CategorySales> results = new LinkedHashMap<>();
    try (KeyValueIterator<Windowed<String>, CategorySales> iter = store.fetchAll(from, to)) {
        while (iter.hasNext()) {
            KeyValue<Windowed<String>, CategorySales> entry = iter.next();
            results.put(entry.key.key(), entry.value);
        }
    }
    ...
}
```

**Interactive Queries explained:**  
In a traditional architecture, aggregation results would be written to a database (Redis, Postgres) and the REST API would query that database. Kafka Streams' Interactive Queries eliminate this entirely — the state store IS the database. The REST endpoint reads directly from the same RocksDB instance that the streaming topology writes to, in the same JVM.

This matters because:
- Zero additional infrastructure (no Redis, no cache layer)
- Sub-millisecond read latency (local memory/disk, no network)
- Data is always fresh — reads happen from the live, continuously-updated store

---

## 8. `ksql/init.sql` — Streaming SQL Definitions

The init.sql file declares ksqlDB objects that mirror and extend the Kafka Streams topology:

```sql
-- STREAM: a ksqlDB view over an existing Kafka topic
-- Every message on the topic becomes a row in the stream
CREATE STREAM IF NOT EXISTS orders_stream (
    orderId VARCHAR, customerId VARCHAR, amount DOUBLE, ...
) WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');

-- Persistent query: STREAM → STREAM (filter)
-- Runs continuously, writes matching records to a new Kafka topic
CREATE STREAM IF NOT EXISTS high_value_orders AS
    SELECT * FROM orders_stream WHERE amount > 500 EMIT CHANGES;

-- Persistent query: STREAM → TABLE (windowed aggregation)
-- Runs continuously, maintains a materialised view of aggregated state
CREATE TABLE IF NOT EXISTS category_sales_1min AS
    SELECT category, COUNT(*) AS order_count, SUM(amount) AS total_sales
    FROM orders_stream
    WINDOW TUMBLING (SIZE 1 MINUTE)
    GROUP BY category EMIT CHANGES;
```

**Key point:** The `TIMESTAMP='timestamp'` clause tells ksqlDB to use the `timestamp` field from the JSON payload as the event time. This aligns ksqlDB's time windows with the Kafka Streams topology's time windows — both use the same event time.

**`IF NOT EXISTS`:** All statements use this clause so the init script is idempotent — safe to run multiple times (on restart, re-deploy, etc.) without errors.

---

## 9. `docker-compose.yml` — Self-Contained Infrastructure

The compose file uses health checks and `depends_on` conditions to enforce the startup order required for the system to work:

```yaml
kafka-setup:
  # Runs kafka-init.sh to create all 5 topics, then exits (code 0)
  depends_on:
    kafka:
      condition: service_healthy   # wait for broker healthcheck to pass

ksqldb-server:
  depends_on:
    kafka-setup:
      condition: service_completed_successfully  # wait for topics to exist first
    schema-registry:
      condition: service_healthy

ksqldb-setup:
  # Pipes init.sql into ksqlDB CLI, then exits (code 0)
  depends_on:
    ksqldb-server:
      condition: service_healthy   # wait for ksqlDB to be fully ready
```

**Why this order matters:**
- ksqlDB `CREATE STREAM` requires the underlying Kafka topic to exist first
- Therefore `kafka-setup` (topic creation) must complete before `ksqldb-server` starts
- `ksqldb-setup` (stream/table creation) must run after `ksqldb-server` is healthy

This means a single `docker compose up -d` results in a fully initialised, ready-to-use stack with no manual steps.

---

## 10. `build.gradle.kts` — Kotlin DSL Build

```kotlin
plugins {
    java
    id("org.springframework.boot") version "3.2.3"    // fat-JAR + bootRun task
    id("io.spring.dependency-management") version "1.1.4"  // Spring BOM
}

java {
    sourceCompatibility = JavaVersion.VERSION_21    // compile with Java 21 features
    targetCompatibility = JavaVersion.VERSION_21    // target JVM 21 bytecode
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")   // REST API
    implementation("org.springframework.boot:spring-boot-starter-actuator")  // /health endpoint
    implementation("org.springframework.kafka:spring-kafka")   // KafkaTemplate + @EnableKafkaStreams
    implementation("org.apache.kafka:kafka-streams")            // KStream, KTable, WindowStore
    implementation("com.fasterxml.jackson.core:jackson-databind")  // JSON serde
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")  // Java time types
    compileOnly("org.projectlombok:lombok")          // @Data, @Builder, @Slf4j
    annotationProcessor("org.projectlombok:lombok")
}
```

**Versions:** Spring Boot 3.2.3's BOM manages all Kafka dependency versions automatically. `spring-kafka` pulls in `kafka-clients 3.6.1` and `kafka-streams 3.6.1`. No explicit version pinning required.

**`gradle.properties`** pins the JDK to ensure `./gradlew` always uses JDK 21 regardless of the system `JAVA_HOME`:
```properties
org.gradle.java.home=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home
```
