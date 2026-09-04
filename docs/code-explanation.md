# Code Explanation
## E-Commerce Kafka Streams + ksqlDB — Technical Walkthrough

This walks through what the code actually does, file by file. The *why* behind the bigger
decisions — and the honest account of what they cost — is in the
[root README](../README.md). Code excerpts here are trimmed for readability; the source
files carry fuller comments.

---

## 1. Project structure and responsibilities

```
config/
  KafkaTopicConfig.java                        NewTopic beans — 6 topics, RF=3 / minISR=2
  KafkaStreamsConfig.java                      All StreamsConfig: EOS_V2, event time,
                                               application.server, per-instance state dir
  OrderTimestampExtractor.java                 Event time from the payload, with fallbacks
  DeadLetterDeserializationExceptionHandler.java  Poison pills → `dead-letter` topic
  InteractiveQueryClientConfig.java            RestClient with bounded timeouts for peer RPC

model/
  Order.java                  Input event (published to `orders`)
  Product.java                Catalog entry (published to `products`, KTable source)
  EnrichedOrder.java          Output of the KStream–KTable join
  OrderAlert.java             Anomaly alert, with a deterministic alertId
  CategorySales.java          Windowed aggregate accumulator (BigDecimal)
  CustomerSpending.java       Per-customer aggregate accumulator (BigDecimal)
  query/                      REST response DTOs, including partial + failures

serde/
  JsonSerde.java              Generic Jackson-based Kafka Serde<T>

producer/
  OrderProducer.java          Publishes Order JSON to `orders`
  ProductProducer.java        @PostConstruct: publishes the 20-product catalog

streams/
  OrderStreamTopology.java    THE MAIN FILE: the complete topology

service/
  InteractiveQueryService.java  Discovery, fan-out, merge, partial-result reporting
  DataSimulator.java            Random order generation for demos

controller/
  OrderController.java        REST: produce orders, control the simulator
  AnalyticsController.java    REST: thin HTTP layer over InteractiveQueryService

exception/
  ApiError.java, ApiExceptionHandler.java, StreamsNotReadyException.java,
  ResourceNotFoundException.java                503 / 400 / 404 semantics
```

There is no `src/test` — the project has no automated tests today.

---

## 2. `application.yml`

```yaml
server:
  port: 8090

spring:
  jackson:
    generator:
      write-bigdecimal-as-plain: true      # 2499.99, never 2.49999E+3
    deserialization:
      use-big-decimal-for-floats: true     # inbound floats bind as BigDecimal, not double

  kafka:
    bootstrap-servers: localhost:9092,localhost:9093,localhost:9094   # all three brokers
    producer:
      key-serializer:   StringSerializer
      value-serializer: StringSerializer   # JSON is serialised by us, sent as a String
      acks: all
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
        transaction.timeout.ms: 60000
    admin:
      fail-fast: false
    streams:
      application-id: ecommerce-streams-app
      bootstrap-servers: localhost:9092,localhost:9093,localhost:9094

app:
  topics: { orders, products, enriched-orders, fraud-alerts, category-sales, dead-letter }
  fraud:
    threshold: 500.0
  streams:
    application-server: ""                 # defaults to localhost:${server.port}
    state-dir: /tmp/kafka-streams/ecommerce
    rpc:
      scheme: http
      connect-timeout-ms: 1000
      read-timeout-ms: 3000
      fan-out-timeout-ms: 5000             # one budget for the WHOLE fan-out
```

**All three brokers are listed.** A single entry would make the whole replicated cluster
unreachable the moment that one broker restarted.

**The ingress producer is not the Streams producer.** `OrderProducer` and `ProductProducer`
use `KafkaTemplate`; the Streams producer is configured separately in
`KafkaStreamsConfig.java` under `EXACTLY_ONCE_V2`. The ingress producer gets `acks=all` and
`enable.idempotence` because exactly-once *inside* the topology is worthless if the records
entering `orders` are already duplicated or already lost. It is **not** transactional — no
`transaction-id-prefix` is set, so `KafkaTemplate` opens no transactions.

**Why not put the Streams config in YAML?** When you declare a
`@Bean(name = DEFAULT_STREAMS_CONFIG_BEAN_NAME)`, Spring Kafka uses that bean and ignores
most `spring.kafka.streams.*` properties. Mixing both produces silent overrides. The
pattern here: minimal YAML (`application-id`, `bootstrap-servers`), everything else in the
Java bean.

Only `app.fraud.threshold` is declared. The velocity, session and baseline thresholds carry
inline defaults in `OrderStreamTopology` and can be overridden as ordinary properties
(`app.fraud.velocity.max-orders`, `app.fraud.session.max-orders`,
`app.fraud.session.max-value`, `app.fraud.baseline.multiplier`,
`app.fraud.baseline.min-orders`).

---

## 3. `KafkaStreamsConfig.java`

`@EnableKafkaStreams` creates a `StreamsBuilderFactoryBean`, exposes a `StreamsBuilder` for
injection into topology builders, and ties the `KafkaStreams` lifecycle to the Spring
context.

`@Value` is on **method parameters**, not fields: in a `@Configuration` class there is no
ordering guarantee that field injection completes before a `@Bean` method runs, whereas
parameter injection is resolved before the method executes.

### The HostInfo bean

```java
@Bean
public HostInfo applicationServerHostInfo(
        @Value("${app.streams.application-server:}") String applicationServer,
        @Value("${server.port:8080}") int serverPort) {
    String endpoint = StringUtils.hasText(applicationServer)
            ? applicationServer.trim() : "localhost:" + serverPort;
    // ... parse host:port, throw IllegalArgumentException on a malformed value
}
```

This is the discovery mechanism the whole multi-instance query story rests on. It is
exposed as a bean so `InteractiveQueryService` resolves "am I the active host for this
key?" against **exactly** the value that was advertised — a second, independently computed
host string would eventually disagree and send an instance into an RPC loop with itself.

### The StreamsConfig properties that matter

| Property | Value | Why |
|---|---|---|
| `PROCESSING_GUARANTEE_CONFIG` | `EXACTLY_ONCE_V2` | One transaction per commit spanning output records, changelog writes and input offsets |
| `producer.enable.idempotence` | `true` | Broker de-duplicates producer retries via (PID, epoch, seq) |
| `producer.acks` | `all` | With `min.insync.replicas=2`, an acked write survives one broker loss |
| `producer.max.in.flight...` | `5` | Idempotence caps this at 5 and preserves per-partition order |
| `producer.transaction.timeout.ms` | `60000` | Streams' 10s default is tight — a long RocksDB flush or GC pause forces an unnecessary abort |
| `consumer.isolation.level` | `read_committed` | Never read records from an aborted or open transaction |
| `REPLICATION_FACTOR_CONFIG` | `3` | Internal changelog/repartition topics; they defaulted to 1 |
| `topic.min.insync.replicas` | `2` | Same, for internal topics |
| `COMMIT_INTERVAL_MS_CONFIG` | `100` | Under EOS the commit interval **is** the transaction size and the visibility-latency floor |
| `CACHE_MAX_BYTES_BUFFERING_CONFIG` | `10 MB` | Was 0, which forced one downstream emission per input record |
| `NUM_STREAM_THREADS_CONFIG` | `2` | Two threads share the task assignment |
| `DEFAULT_TIMESTAMP_EXTRACTOR...` | `OrderTimestampExtractor` | Window on the payload's event time, the same clock ksqlDB uses |
| `AUTO_OFFSET_RESET_CONFIG` | `earliest` | Process from the beginning on first run |
| `DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER...` | `DeadLetterDeserializationExceptionHandler` | Was `LogAndContinue`, which discarded the bytes |
| `APPLICATION_SERVER_CONFIG` | `host:port` | Published through group metadata so peers can be discovered |
| `STATE_DIR_CONFIG` | `<base>/<host>-<port>` | RocksDB takes an exclusive lock; a shared path stops two JVMs on one machine |

The state directory point is worth spelling out: with a single hardcoded path, a second JVM
on the same machine dies with "Failed to lock the state directory" — meaning the very
multi-instance topology the RPC fan-out exists to serve could not be brought up locally at
all. The `application-id` is still the shared identity that groups the instances; only the
on-disk location differs.

---

## 4. `OrderTimestampExtractor.java`

```java
public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
    long eventTime = payloadTimestamp(record.value());   // Order or EnrichedOrder
    if (eventTime > 0) return eventTime;

    long recordTime = record.timestamp();                // broker append time
    if (recordTime > 0) return recordTime;

    if (partitionTime >= 0) return partitionTime;        // highest timestamp seen so far
    return System.currentTimeMillis();                   // last resort
}
```

Without this, Kafka Streams uses `FailOnInvalidTimestamp` over the record's broker
timestamp, while `ksql/init.sql` declares `TIMESTAMP='timestamp'` — the payload's event
time. The two engines bucketed the same order into different minutes, so no comparison
between them meant anything.

**Why it degrades instead of throwing:** a `TimestampExtractor` that throws kills the
`StreamThread`. One malformed record should not stop the application; the fallbacks are
ordered from most to least faithful, and each non-payload path logs.

---

## 5. `JsonSerde.java`

```java
private static final ObjectMapper MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
        .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
```

Kafka ships serdes for `String`, `Integer`, `Double` and so on, but nothing for POJOs. One
`JsonSerde<T>` per type is created at topology-build time (not per record); the
`ObjectMapper` is static, shared and configured at construction, which makes it
thread-safe.

The two `BigDecimal` settings are what keep money exact end-to-end: inbound JSON floats
bind as `BigDecimal` rather than lossy `double`, and outbound values serialise as plain
decimals rather than scientific notation (which ksqlDB's `DOUBLE` columns would still read,
but which is unreadable in Kafka UI).

Deserialization failures throw `SerializationException`, which is what routes the record to
`DeadLetterDeserializationExceptionHandler`.

---

## 6. `OrderStreamTopology.java`

`@Autowired void buildTopology(StreamsBuilder builder)` is called by Spring before
`KafkaStreams.start()`.

### 6.1 Sources

```java
KStream<String, Order> ordersStream = builder.stream(
        ordersTopic, Consumed.with(Serdes.String(), orderSerde).withName("orders-source"));

KTable<String, Product> productsTable = builder.table(
        productsTopic,
        Consumed.with(Serdes.String(), productSerde),
        Materialized.<String, Product, KeyValueStore<Bytes, byte[]>>as(PRODUCTS_STORE)
                .withKeySerde(Serdes.String()).withValueSerde(productSerde));
```

`builder.stream()` gives a `KStream` — an unbounded sequence of independent facts.
`builder.table()` gives a `KTable` — the latest value per key, materialised in RocksDB. The
`products` topic is compacted, so Kafka retains only the most recent record per
`productId`, which is exactly a KTable's semantics on disk.

### 6.2 Enrichment join

```java
KStream<String, Order> ordersByProduct = ordersStream
        .selectKey((orderId, order) -> order.getProductId(), Named.as("rekey-by-productId"));

KStream<String, EnrichedOrder> enrichedByProduct = ordersByProduct.leftJoin(
        productsTable, EnrichedOrder::from,
        Joined.with(Serdes.String(), orderSerde, productSerde).withName("orders-products-join"));

KStream<String, EnrichedOrder> enrichedStream = enrichedByProduct
        .selectKey((productId, enriched) -> enriched.getOrderId(), Named.as("rekey-by-orderId"));

enrichedStream.to(enrichedOrdersTopic, Produced.with(Serdes.String(), enrichedOrderSerde)
        .withName("enriched-orders-sink"));
```

Orders arrive keyed by `orderId` but the KTable is keyed by `productId`, so the stream must
be re-keyed; `selectKey()` marks it for repartitioning and Kafka Streams creates the
internal topic.

**Why `leftJoin` and not `join`?** The catalog is published at startup, so an order can
arrive before the KTable has caught up. A strict join would silently drop that order;
`leftJoin` passes `null` and `EnrichedOrder.from()` substitutes `"Unknown Product"` /
`"Unknown Brand"`.

**Known gap:** this is a plain `KTable`, not a versioned store, so the lookup resolves
against whatever product version is in the table at *processing* time. Replaying an old
order enriches it with today's catalog. The aggregation half of this topology is
event-time; this half is not. See the root README's limitations section.

### 6.3 Category aggregation — grace and suppression

```java
KTable<Windowed<String>, CategorySales> categorySalesTable = enrichedStream
        .groupBy((orderId, order) -> order.getCategory(),
                 Grouped.<String, EnrichedOrder>as("grouped-by-category")
                        .withKeySerde(Serdes.String()).withValueSerde(enrichedOrderSerde))
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(30)))
        .aggregate(
                CategorySales::new,
                (category, order, agg) -> agg.update(category, order.getAmount(), order.getQuantity()),
                Materialized.<String, CategorySales, WindowStore<Bytes, byte[]>>as(CATEGORY_SALES_STORE)
                        .withKeySerde(Serdes.String()).withValueSerde(categorySalesSerde));

categorySalesTable
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream(Named.as("category-sales-stream"))
        .map((windowedKey, sales) -> KeyValue.pair(windowedKey.key(), sales))
        .to(categorySalesTopic, Produced.with(Serdes.String(), categorySalesSerde)
                .withName("category-sales-sink"));
```

Three deliberate choices:

1. **`ofSizeAndGrace(1 min, 30 s)`**, not `ofSizeWithNoGrace`. With zero grace, a record
   whose event time landed in an already-closed window was dropped from the aggregate — yet
   the same record still reached `enriched-orders`, still fired an alert and still
   incremented customer spending. One late order made three outputs disagree.
2. **`suppress(untilWindowCloses(...))`** so each window emits exactly one final record
   rather than an update per input record. The trade-off is latency: nothing is emitted
   until window end + grace has passed *in stream time*, so an idle stream leaves the last
   window unemitted. Anything needing sub-window freshness reads the state store over REST.
3. **The store is materialised under a name** (`category-sales-store`) so Interactive
   Queries can read the in-progress aggregate even though the topic only carries final
   results.

### 6.4 One explicit repartition by customerId

```java
KStream<String, EnrichedOrder> ordersByCustomer = enrichedStream
        .selectKey((orderId, order) -> order.getCustomerId(), Named.as("rekey-by-customerId"))
        .repartition(Repartitioned.<String, EnrichedOrder>as("orders-by-customer")
                .withKeySerde(Serdes.String()).withValueSerde(enrichedOrderSerde));
```

Everything customer-scoped below — the spending KTable and three of the four signals —
needs `customerId` as the key. Doing `selectKey` + `repartition` **once, explicitly**,
instead of letting four separate `groupBy(customerId)` calls each mark the stream dirty,
buys two things:

- **One** internal repartition topic instead of four copies of the same data.
- **Co-partitioning for free.** The `BASELINE_DEVIATION` stream–table join joins this
  stream against a KTable derived from the same repartition topic, so both sides have
  identical keys and identical partition counts — the co-partitioning requirement Kafka
  Streams enforces at startup.

### 6.5 Customer spending KTable

```java
KTable<String, CustomerSpending> customerSpendingTable = ordersByCustomer
        .groupByKey(Grouped.<String, EnrichedOrder>as("grouped-by-customer")
                .withKeySerde(Serdes.String()).withValueSerde(enrichedOrderSerde))
        .aggregate(
                CustomerSpending::new,
                (customerId, order, agg) -> agg.update(customerId, order.getAmount(), order.getTimestamp()),
                Materialized.<String, CustomerSpending, KeyValueStore<Bytes, byte[]>>as(CUSTOMER_SPENDING_STORE)
                        .withKeySerde(Serdes.String()).withValueSerde(customerSpendingSerde));
```

Already keyed and repartitioned, so `groupByKey()` adds no second shuffle. Unwindowed on
purpose — it is the lifetime baseline `BASELINE_DEVIATION` needs. The store **name** is
unchanged from the original `Serdes.Double()` version so Interactive Queries kept
resolving; only the value type changed, from a raw `Double` to a typed `CustomerSpending`
carrying `customerId`, `totalSpent`, `orderCount`, `avgOrderValue` and
`lastOrderTimestamp`.

The old version ran `BigDecimal.valueOf(total).add(amount).doubleValue()` — which
re-introduced binary rounding error on every record it was meant to prevent.

This store is unbounded and has no retention policy. See the root README.

### 6.6 The four anomaly signals

**SIGNAL 1 — `HIGH_VALUE`.** A stateless filter, kept but demoted to one signal of four.

```java
ordersByCustomer
        .filter((customerId, order) -> order.getAmount() != null
                && order.getAmount().compareTo(fraudThreshold) > 0, Named.as("high-value-filter"))
        .mapValues(order -> OrderAlert.fromEnrichedOrder(order, "HIGH_VALUE"), ...);
```

**SIGNAL 2 — `VELOCITY`**, over a hopping window (5 minutes, advancing every minute, 1
minute grace). A *tumbling* window resets on a fixed boundary — three orders at 11:59 and
three more at 12:01 would never be seen together. Hopping windows overlap, so every minute
there is a window covering the trailing five.

The cost is fan-out: each record belongs to `size / advance` = 5 windows at once, so a
naive filter would emit the same burst five times. `isTrailingWindow` keeps only the oldest
window containing the record:

```java
private static boolean isTrailingWindow(Windowed<String> windowedKey, long eventTimestamp) {
    if (eventTimestamp <= 0) return false;
    long advanceMs = VELOCITY_ADVANCE.toMillis();
    long trailingStart = (eventTimestamp / advanceMs) * advanceMs
            - (VELOCITY_WINDOW.toMillis() - advanceMs);
    return windowedKey.window().start() == trailingStart;
}
```

This branch is deliberately **not** suppressed: a velocity alert arriving six minutes after
the burst is useless, so it emits on update and re-alerts as a burst grows.

**SIGNAL 3 — `SESSION_BURST`**, over a session window with a 5-minute inactivity gap. A
session groups activity by *activity*, not by the clock — it runs until the customer goes
quiet, however long that takes. That is the natural unit for "was this shopping session as
a whole abnormal?", which no fixed-size window can ask.

Session windows need a **merger**, because a late record can bridge the gap between two
sessions and Streams must combine their aggregates:

```java
.aggregate(CustomerSpending::new,
           (customerId, order, agg) -> agg.update(...),
           (customerId, left, right) -> CustomerSpending.merge(left, right),
           Materialized.<...>as(CUSTOMER_SESSION_STORE)...)
.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
```

`CustomerSpending.merge` recomputes the average from the *combined totals* rather than
averaging two averages. This branch **is** suppressed: a session's verdict is only
meaningful once the session has ended.

**SIGNAL 4 — `BASELINE_DEVIATION`**, a stream–table join asking "is this order abnormal for
*this* customer?"

```java
KStream<String, OrderAlert> baselineAlerts = ordersByCustomer
        .leftJoin(customerSpendingTable, this::baselineDeviationAlert,
                  Joined.with(Serdes.String(), enrichedOrderSerde, customerSpendingSerde)
                        .withName("baseline-deviation-join"))
        .filter((customerId, alert) -> alert != null, Named.as("baseline-deviation-filter"));
```

Two subtleties:

- **Self-inclusion.** `customerSpendingTable` is built from the same stream and its
  aggregate node was added to the topology *before* this join node. Kafka Streams forwards
  a record to a node's children in the order they were added, depth first, so by the time
  the join runs the store already contains the current order. `baselineDeviationAlert`
  therefore subtracts the current order back out before comparing.
- **A minimum history guard.** `priorOrderCount = spending.getOrderCount() - 1` must be at
  least `baselineMinOrders` (5). Without it, a customer's very first order is trivially "3×
  the average of nothing" and every new customer gets flagged.

**Merge.** All four streams are keyed by `customerId`, so every alert about one customer
lands on one partition in order — what an alert consumer wants when correlating signals.
(The high-value branch used to be keyed by `orderId`.)

```java
highValueAlerts
        .merge(baselineAlerts, Named.as("merge-baseline-alerts"))
        .merge(velocityAlerts, Named.as("merge-velocity-alerts"))
        .merge(sessionAlerts,  Named.as("merge-session-alerts"))
        .to(fraudAlertsTopic, Produced.with(Serdes.String(), alertSerde).withName("fraud-alerts-sink"));
```

Reusing one topic (rather than four) keeps `KafkaTopicConfig` and the ksqlDB
`fraud_alerts_stream` schema untouched; consumers discriminate on `alertType`.

### 6.7 Deterministic alert identity

```java
public static String deterministicAlertId(String identity, String alertType, String severity) {
    String name = "urn:ecommerce:order-alert:" + identity + "|" + alertType + "|" + severity;
    return UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8)).toString();
}
```

`identity` is the `orderId` for per-order signals and `customerId + "@" + window.start()`
for the two windowed ones. Timestamps are event time — the order's own timestamp, or the
last order time folded into the window's aggregate.

Previously these were `UUID.randomUUID()` and `System.currentTimeMillis()`, so a replayed
order produced a second alert that was indistinguishable from a genuine new incident:
ksqlDB's `fraud_by_category COUNT(*)` double-counted, and an on-call rotation would be
paged twice for one event.

**Why the window is part of the identity:** keying on `customerId` alone would collapse
every window that customer ever triggers onto a single alert id, and a consumer deduping on
`alertId` would then drop every burst after the first — worse than the random ids it
replaced.

---

## 7. `CategorySales.java` and `CustomerSpending.java`

Mutable accumulators with `BigDecimal` money at scale 2, `HALF_UP`:

```java
public CategorySales update(String category, BigDecimal amount, int quantity) {
    BigDecimal value = money(amount == null ? BigDecimal.ZERO : amount);
    this.category = category;
    this.orderCount++;
    this.totalSales = money(this.totalSales.add(value));
    this.totalQuantity += quantity;
    this.avgOrderValue = this.totalSales.divide(
            BigDecimal.valueOf(this.orderCount), 2, RoundingMode.HALF_UP);
    if (this.maxOrderValue == null || value.compareTo(this.maxOrderValue) > 0) this.maxOrderValue = value;
    if (this.minOrderValue == null || value.compareTo(this.minOrderValue) < 0) this.minOrderValue = value;
    return this;
}
```

**Why mutable?** `aggregate()` deserialises the accumulator from RocksDB, hands it to the
aggregator, and serialises the returned value back. Updating in place avoids an allocation
per record and is the standard Kafka Streams pattern. `@NoArgsConstructor` is required
because `CategorySales::new` is the initialiser and Jackson needs it to deserialise.

**Why these were `double`.** `totalSales += amount` runs once per record, so binary
rounding error compounded across every order in a window — the reported SUM and AVG drifted
further from truth the busier the window got. A money bug that is invisible at small volume
and grows with load is the worst possible shape.

**`merge()` matters twice.** Session windows merge two aggregates when a late record bridges
a gap, and the REST fan-out merges shards from different instances. Both recompute the
average from the summed totals rather than averaging averages, and `minOrderValue` starts
as `null` rather than `Double.MAX_VALUE` so an empty aggregate is distinguishable from a
real minimum.

---

## 8. `InteractiveQueryService.java` — multi-instance queries

A Kafka Streams state store is **sharded by partition**: each instance materialises state
only for the partitions it was assigned, so `store.all()` and `store.fetchAll()` iterate
one instance's slice. There is no cluster-wide iterator. The previous controller called
those methods directly and returned the result as the whole answer — correct with one
instance, and roughly half the data with two, at HTTP 200 with no warning.

### Two query shapes, two strategies

**Keyed lookup** — `queryMetadataForKey` runs the key through the same partitioner the
producer used and names the single owning instance:

```java
KeyQueryMetadata metadata = streams.queryMetadataForKey(
        CUSTOMER_SPENDING_STORE, customerId, KEY_SERIALIZER);

if (metadata == null || KeyQueryMetadata.NOT_AVAILABLE.equals(metadata)) {
    throw new StreamsNotReadyException("Partition ownership ... not available yet");   // → 503
}
if (isLocal(metadata.activeHost())) return readLocalCustomerSpending(streams, customerId);
// else exactly one HTTP hop to that host, with ?local=true
```

`KEY_SERIALIZER` must be the same serializer the topology keys with (`Serdes.String()`), or
the computed partition is not the partition the record landed in and the lookup goes to the
wrong host. `NOT_AVAILABLE` means the assignment is genuinely in flux — answering from the
local store would be the same silent-wrong-answer failure this class exists to remove, so
it 503s.

**Unkeyed scan** — `streamsMetadataForStore` lists every instance hosting the store; all of
them are queried in parallel on virtual threads and merged:

```java
for (StreamsMetadata instance : metadata) {
    HostInfo host = instance.hostInfo();

    // Skip standby-only hosts: a standby is a full COPY of an active's partitions, not a
    // different shard, so merging it would DOUBLE every figure it duplicated.
    if (!instance.stateStoreNames().contains(storeName)
            && instance.standbyStateStoreNames().contains(storeName)) continue;

    if (isUnavailable(host)) { failures.add(... "NO_APPLICATION_SERVER" ...); continue; }

    if (isLocal(host)) queryLocal = true;
    else remoteCalls.put(host, CompletableFuture.supplyAsync(() -> remoteReader.apply(host), fanOutExecutor));
}
```

The local read runs on the calling thread while the remote calls are in flight, so a
single-instance deployment costs no thread hand-off and a multi-instance one overlaps its
RocksDB scan with the network wait. The timeout is a **single budget for the whole
fan-out**, not per peer — three dead peers with a 3s timeout each must not add up to a 9s
request.

Virtual threads suit this exactly: the work is blocking I/O against a handful of peers, the
count scales with deployment size, and nothing is CPU-bound.

### Recursion guard

The remote leg calls the *same* public endpoints with `local=true`, which makes the peer
read only its own partitions and skip discovery. Without that flag every instance would fan
out to every other instance, which would fan out again — an infinite recursion that
saturates the cluster on the first request.

### Merging

Merges are by **full identity** and they **sum**; they never pick a winner:

- customer spending merges by `customerId`
- category sales merges by `(category, windowStart, windowEnd)`

Under normal partitioning each key appears in exactly one shard, so this is usually a
concatenation. Summing matters when a scan overlaps a rebalance and legitimately sees the
same key twice — and "pick one" is exactly how the old category-sales code lost windows.

### Failure reporting

```java
if (results.isEmpty()) throw new StreamsNotReadyException("No instance could serve store ...");  // 503
return new FanOut<>(results, hosts, failures);   // partial() == !failures.isEmpty()
```

Every failure mode downgrades the answer rather than corrupting it, and the reason is
classified so a client can branch: `REBALANCING`, `STORE_NOT_AVAILABLE`, `RPC_FAILED`,
`TIMEOUT`, `INTERRUPTED`, `NO_APPLICATION_SERVER`, `QUERY_FAILED`. If every host fails, an
empty list with `partial: true` would be technically honest but read as "there is no data",
so the request 503s instead.

`status()` never throws and never 503s — it is the endpoint an operator calls to find out
*why* the query endpoints are 503ing, so it has to answer while the client is `REBALANCING`
or absent.

---

## 9. `AnalyticsController.java` and the exception handler

The controller is a thin HTTP layer: parameter validation, defaulting, and delegation.

```java
Instant rangeEnd   = (to != null) ? to : Instant.now();
Instant rangeStart = (from != null) ? from : rangeEnd.minus(Duration.ofMinutes(windowMinutes));
if (rangeStart.isAfter(rangeEnd)) throw new IllegalArgumentException(...);   // → 400
```

`from`/`to` are **event time**, matched against window start times. The old code built its
range from `Instant.now()` and queried an event-time store with it — which agrees only
while the app is live and caught up. After a restart, `auto.offset.reset=earliest` replays
the topic and fills the store with windows whose event times are in the past, so the
endpoint returned an empty list and the app looked dead while working perfectly.

`ApiExceptionHandler` (`@RestControllerAdvice`) maps:

| Exception | Status | Code |
|---|---|---|
| `StreamsNotReadyException` | `503` + `Retry-After` | `STREAMS_NOT_READY` |
| `InvalidStateStoreException` | `503` + `Retry-After` | `STATE_STORE_NOT_AVAILABLE` |
| `ResourceNotFoundException`, `NoHandlerFoundException` | `404` | `NOT_FOUND` |
| `IllegalArgumentException`, `DateTimeParseException`, binding errors | `400` | `BAD_REQUEST` |
| anything else | `500` | `INTERNAL_ERROR` |

Before this existed, a routine rebalance threw a raw `IllegalStateException` into Spring's
default handler and rendered as a `500` with a stack trace — telling every monitor that a
normal lifecycle event was a server failure, and leaking internals into the response body.

`readLocalCustomerSpending` also fixes a type bug worth remembering: the old controller
declared `ReadOnlyKeyValueStore<String, Double>` for a store holding `CustomerSpending`.
Generic **erasure** meant that compiled cleanly and threw `ClassCastException` on the first
record at runtime, because the cast lives at the call site, not in the store. No compiler
check would have caught it — only matching the declared type to the topology does.

---

## 10. `DeadLetterDeserializationExceptionHandler.java`

On a deserialization failure, the raw key and value bytes are republished to `dead-letter`
with metadata headers, and the handler returns `CONTINUE`:

| Header | Contents |
|---|---|
| `dlq.original.topic` / `.partition` / `.offset` / `.timestamp` | Where the record came from |
| `dlq.exception.class` / `.message` | What failed, including the root cause chain |
| `dlq.application.id` | Which Streams app rejected it |
| `dlq.failed.at` | Wall-clock time of the failure |

**The producer is deliberately separate and non-transactional.** Kafka Streams does not
expose the task's transactional producer to a `DeserializationExceptionHandler`, and a
record that failed to deserialize is precisely the record whose processing transaction is
about to be abandoned — enrolling the DLQ write in it would abort the DLQ write too. The
handler's producer is idempotent with `acks=all`, but outside the Streams transaction.

**Consequence:** the DLQ is at-least-once. A crash between the DLQ write and the offset
commit re-delivers the record and writes it a second time; a crash the other way can orphan
it. Dedupe on `(dlq.original.topic, dlq.original.partition, dlq.original.offset)`.

The producer is a static singleton because `configure()` may be called once per StreamThread
and each `KafkaProducer` holds its own connections; `send()` is followed by `flush()` so the
record is durable before the handler returns.

---

## 11. `ksql/init.sql` — streaming SQL

```sql
CREATE STREAM IF NOT EXISTS orders_stream (
    orderId VARCHAR, customerId VARCHAR, productId VARCHAR, category VARCHAR,
    amount DOUBLE, quantity INT, status VARCHAR, timestamp BIGINT
) WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='JSON', TIMESTAMP='timestamp');

CREATE TABLE IF NOT EXISTS category_sales_1min AS
    SELECT category, COUNT(*) AS order_count, SUM(amount) AS total_sales,
           AVG(amount) AS avg_order_value, MAX(amount) AS max_order_value,
           SUM(quantity) AS total_items
    FROM orders_stream
    WINDOW TUMBLING (SIZE 1 MINUTE)
    GROUP BY category EMIT CHANGES;
```

`TIMESTAMP='timestamp'` tells ksqlDB to use the payload's event time — the same field
`OrderTimestampExtractor` reads, which is what makes the two engines comparable.
`IF NOT EXISTS` makes the script idempotent, so `ksqldb-setup` can re-run on every
`docker compose up`.

Three honest caveats about this layer:

- **`amount` is `DOUBLE`.** The Java side moved to `BigDecimal` precisely because float
  error compounds across a window's SUM. The ksqlDB aggregates still carry it, so the two
  engines agree closely rather than exactly.
- **`fraud_by_category` misses two of four signals.** It groups by `category`, and
  `VELOCITY` / `SESSION_BURST` alerts leave `category` null by design — ksqlDB drops null
  grouping keys.
- **`fraud_alerts_stream` predates the multi-signal work.** It has no `reason` column, so
  the text justifying an aggregate alert is invisible in SQL.

`ksql/queries.sql` also contains stale statements — snake_case column names that `init.sql`
never declares (`order_id`, `customer_id`, `product_name`) and a `customer_spending_total`
table that does not exist.

---

## 12. `docker-compose.yml`

Three brokers on `9092` / `9093` / `9094` from the host, and `kafka-1:29092`,
`kafka-2:29093`, `kafka-3:29094` inside the compose network, with
`KAFKA_DEFAULT_REPLICATION_FACTOR: 3`, `KAFKA_MIN_INSYNC_REPLICAS: 2` and — importantly —
`KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3` / `..._MIN_ISR: 2`. Exactly-once is
implemented with Kafka transactions whose state lives in `__transaction_state`; at RF=1,
losing the one broker would lose the transaction log.

Log retention is 168 hours (7 days), up from 2, so replay and reprocessing are actually
possible. Heaps are capped (512 MB per broker, 768 MB for ksqlDB) so the stack fits
alongside the app and an IDE on a 16 GB machine.

The startup ordering is enforced with health checks:

```yaml
kafka-setup:        depends_on: kafka-1/2/3  condition: service_healthy
schema-registry:    depends_on: kafka-setup  condition: service_completed_successfully
ksqldb-server:      depends_on: kafka-setup + schema-registry
ksqldb-setup:       depends_on: ksqldb-server condition: service_healthy
```

`CREATE STREAM` requires the underlying topic to exist, so topic creation must complete
before ksqlDB starts, and stream creation must run after ksqlDB is healthy. One
`docker compose up -d` therefore yields a fully initialised stack.

Schema Registry is started and ksqlDB is pointed at it, but **nothing uses it** —
serialisation is the hand-rolled `JsonSerde` and ksqlDB uses `VALUE_FORMAT='JSON'`. There
is no schema evolution enforcement anywhere in this system.

---

## 13. `build.gradle.kts`

```kotlin
plugins {
    java
    id("org.springframework.boot") version "3.2.3"
    id("io.spring.dependency-management") version "1.1.4"
}

java {
    // Toolchain (not sourceCompatibility) so Gradle resolves or provisions a JDK 21
    // on any machine, instead of depending on whatever JVM happens to be on PATH.
    toolchain { languageVersion = JavaLanguageVersion.of(21) }
}
```

The toolchain replaced a `gradle.properties` entry that pinned `org.gradle.java.home` to a
hardcoded macOS JDK path — which worked on exactly one machine. If Gradle cannot find or
provision a JDK 21, set `JAVA_HOME` to one.

Spring Boot 3.2.3's BOM manages the Kafka versions; `spring-kafka` pulls in `kafka-clients`
and `kafka-streams`, so no explicit pinning is needed. `kafka-streams-test-utils` and
`spring-kafka-test` back a 47-test suite under `src/test/`: `TopologyTestDriver` tests for the
windowing, money, enrichment and anomaly-detection behaviour, stubbed-transport tests for the
Interactive Query fan-out, and one Testcontainers end-to-end test tagged `integration` that is
excluded from `./gradlew test` so the default build stays green without a Docker daemon.
Run the tagged test with `./gradlew integrationTest`.
