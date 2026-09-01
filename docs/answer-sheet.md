# Assignment Answer Sheet
## Exploration of Streaming Architecture — Apache Kafka Streams & ksqlDB

**Reference:** https://docs.confluent.io/platform/current/streams/overview.html

---

## 1. Group Information

| Field | Details |
|-------|---------|
| **Case Study Title** | Real-Time E-Commerce Order Analytics with Kafka Streams and ksqlDB |
| **Platform Explored** | Apache Kafka Streams + Confluent ksqlDB |
| **Technology Stack** | Java 21, Spring Boot 3.2, Gradle (Kotlin DSL), Docker / Colima |

*(Add group number, member IDs and names here)*

---

## 2. Abstract

Modern e-commerce platforms generate thousands of order events per second. Traditional batch processing — running nightly jobs to compute sales, detect fraud, or update dashboards — introduces latency measured in hours and misses time-critical scenarios like real-time fraud interception.

This project explores **Apache Kafka Streams** and **ksqlDB** as two complementary approaches to stream processing over the same Kafka infrastructure. Using a simulated e-commerce order pipeline as the use case, the system demonstrates:

- **Kafka Streams Java DSL**: A library embedded inside the application that performs stateful, fault-tolerant stream processing — including KStream-KTable joins, windowed aggregations, fraud detection filters, and RocksDB-backed interactive queries
- **ksqlDB**: A SQL-based streaming layer on top of the same Kafka topics, enabling analysts to write continuous queries (`EMIT CHANGES`), materialised views, and time-windowed aggregations without writing Java code

The project uses Spring Boot as the REST-API and producer layer, with Docker Compose orchestrating the full Confluent Platform stack. Every order placed through the API flows through the Kafka Streams topology in milliseconds, and the same data is simultaneously queryable via ksqlDB push and pull queries.

---

## 3. Architecture Details

### 3.1 High-Level Flow

```
[Customer / Simulator]
        │  POST /api/orders
        ▼
[Spring Boot :8090]
  ├─ OrderProducer  ──────────────────► orders topic (key=orderId)
  └─ ProductProducer (@PostConstruct) ► products topic (key=productId, compact)

[Kafka Streams Topology]  (runs inside Spring Boot JVM)
  ├─ Source: orders KStream ──────────────────────────────────────┐
  ├─ Source: products KTable (local RocksDB lookup)               │
  │                                                               ▼
  ├─ STEP 1: selectKey(productId) → re-partition                  │
  ├─ STEP 2: leftJoin(productsTable) ─► EnrichedOrder ──► enriched-orders topic
  ├─ STEP 3: filter(amount > $500)   ─► OrderAlert    ──► fraud-alerts topic
  ├─ STEP 4: groupBy(category)                                    │
  │           .windowedBy(Tumbling 1-min)                         │
  │           .aggregate(CategorySales) ──────────────────────────► category-sales topic
  └─ STEP 5: groupBy(customerId)                                  │
              .aggregate(totalSpent) ───────────────────────────► customer-spending-store (RocksDB)

[ksqlDB :8088]  (reads same topics, independent process)
  ├─ STREAM orders_stream               ← orders topic
  ├─ STREAM enriched_orders_stream      ← enriched-orders topic
  ├─ STREAM fraud_alerts_stream         ← fraud-alerts topic
  ├─ STREAM high_value_orders           ← persistent query: filter amount>500
  ├─ TABLE  category_sales_1min         ← persistent query: TUMBLING 1-min window
  ├─ TABLE  category_sales_hourly       ← persistent query: HOPPING 1-hr window
  ├─ TABLE  customer_spending           ← persistent query: running totals
  └─ TABLE  fraud_by_category           ← persistent query: fraud aggregation

[Spring Boot REST /api/analytics/*]
  └─ AnalyticsController queries Kafka Streams Interactive Stores directly (no DB)
```

### 3.2 Kafka Topics and Their Roles

| Topic | Type | Key | Value | Written By |
|-------|------|-----|-------|-----------|
| `orders` | Regular | orderId | Order JSON | Spring Boot producer |
| `products` | Compacted | productId | Product JSON | Spring Boot @PostConstruct |
| `enriched-orders` | Regular | orderId | EnrichedOrder JSON | Kafka Streams topology |
| `fraud-alerts` | Regular | orderId | OrderAlert JSON | Kafka Streams topology |
| `category-sales` | Regular | category | CategorySales JSON | Kafka Streams topology |

**Compacted topic** (`products`): Kafka retains only the latest message per key. This makes it suitable as a KTable source — Kafka Streams reads the full changelog to reconstruct the latest product catalog in local state.

### 3.3 Stream Processing Guarantees

- **Exactly-once semantics**: Kafka Streams uses idempotent producers + atomic offset commits to ensure each record is processed exactly once even on restart
- **Fault tolerance**: State stores (RocksDB) are backed by internal changelog Kafka topics. On restart, Kafka Streams rebuilds state from the changelog
- **Ordered processing**: Within a partition, records maintain Kafka's ordering guarantee
- **Event time**: Window aggregations use the `timestamp` field embedded in the JSON payload (event time), not wall-clock time, so late arrivals are placed in the correct window

---

## 4. Use Cases Where Kafka Streams / ksqlDB Are Suitable

### Why streaming over batch for e-commerce?

| Requirement | Batch (nightly job) | Kafka Streams / ksqlDB |
|-------------|---------------------|------------------------|
| Fraud detection | Too late — order already fulfilled | Sub-second alert on every order |
| Live dashboard | Stale by hours | Refreshed per-minute or per-event |
| Order enrichment | Separate ETL pipeline | In-flight join, zero latency |
| Customer analytics | Aggregated next day | Running totals updated per event |
| System coupling | Tight ETL dependencies | Decoupled via Kafka topics |

### Domains Well-Suited for This Architecture

| Domain | Specific Use Case |
|--------|------------------|
| **Financial services** | Real-time transaction fraud, regulatory reporting, FX rate aggregation |
| **E-commerce** | Order analytics, recommendation engines, inventory updates |
| **IoT / Telemetry** | Sensor anomaly detection, device state tracking, threshold alerting |
| **Logistics** | Package tracking, route optimisation, ETA computation |
| **Healthcare** | Patient vital monitoring, medication alerts, clinical event correlation |
| **Social / Media** | Trending topics (windowed COUNT), engagement metrics, content moderation |
| **Telecommunications** | Call detail record (CDR) aggregation, network fault detection |

### When to Choose Kafka Streams vs ksqlDB

| Criteria | Kafka Streams Java API | ksqlDB |
|----------|----------------------|--------|
| Complex business logic | ✅ Full Java expressiveness | ❌ SQL only |
| Team SQL expertise | ❌ Requires Java developers | ✅ SQL-familiar analysts |
| Custom serdes / formats | ✅ Any Jackson/Avro/Protobuf | Limited |
| Interactive state queries | ✅ Direct RocksDB access | ❌ |
| Rapid prototyping | Slower | ✅ Write SQL in minutes |
| Operational complexity | Embedded in app | Separate service |
| Fine-grained error handling | ✅ DeserializationExceptionHandler | Limited |

**Key insight**: In this project, both layers run simultaneously on the same topics. Kafka Streams handles the complex join and stateful aggregation logic in Java; ksqlDB provides a SQL layer for exploration, dashboarding, and analyst access — neither replaces the other.

---

## 5. Kafka Streams — Deep Exploration

### 5.1 What Kafka Streams Provides

Kafka Streams is a **client library** (not a separate cluster) that turns a JVM application into a stateful stream processor. It is imported as a dependency and runs inside the same JVM as the Spring Boot application. There is no separate processing cluster to operate.

**Core programming model:** A **topology** of source nodes (Kafka topics), processing nodes (transformations), and sink nodes (output topics).

```
Source Processor ──► Stream Processor ──► Stream Processor ──► Sink Processor
(Kafka topic)        (stateless/stateful)   (stateless/stateful)  (Kafka topic)
```

### 5.2 KStream vs KTable — The Duality of Streams and Tables

This is the foundational concept in Kafka Streams and stream processing theory:

**KStream** — an event stream
- Every record is an independent fact. A new record **appends** to the log.
- Think: order placed, click event, sensor reading
- In this project: `orders` topic → `KStream<String, Order>`
- The stream is infinite — it never ends

**KTable** — a changelog stream / materialised view
- Every record **updates** the latest value for a key. Only the most recent record per key matters.
- Think: user profile, product price, inventory level
- In this project: `products` topic → `KTable<String, Product>`
- The compacted `products` topic ensures Kafka only keeps the latest product per productId

**The duality**: A KStream can be turned into a KTable (by grouping and aggregating), and a KTable can be seen as a KStream where each update emits a change event. This bidirectionality is what makes Kafka Streams powerful — time-windowed aggregations naturally produce KTables that are later read as KStreams to write to output topics.

### 5.3 KStream–KTable Join (Order Enrichment)

```java
// Re-key orders by productId so the key matches the KTable
KStream<String, Order> ordersByProduct = ordersStream
    .selectKey((orderId, order) -> order.getProductId());

// Left join: for each order, look up the product
KStream<String, EnrichedOrder> enriched = ordersByProduct.leftJoin(
    productsTable,
    EnrichedOrder::from,                       // ValueJoiner lambda
    Joined.with(Serdes.String(), orderSerde, productSerde)
);
```

**Why this is powerful:**
- The `products` KTable is held in a local RocksDB state store (one store per partition)
- The join happens **in-memory** without a database roundtrip — O(1) lookup
- `leftJoin` means if no product exists for a productId, the join still succeeds (product = null)
- Kafka Streams automatically handles the repartitioning triggered by `selectKey()` using an internal topic

This pattern replaces a traditional synchronous database lookup on the order-processing hot path with a pre-materialised local cache, eliminating latency spikes.

### 5.4 Windowed Aggregations

Kafka Streams supports four window types. This project uses Tumbling:

```
TUMBLING (SIZE 1 min):
Timeline: ─────────────────────────────────────────────────►
           [0:00────0:59] [1:00────1:59] [2:00────2:59]
           Window 1       Window 2       Window 3
           (independent, non-overlapping)

HOPPING (SIZE 1hr, ADVANCE 15min):
           [0:00─────1:00]
                [0:15─────1:15]
                     [0:30─────1:30]   ← same event in 4 windows
```

```java
enrichedStream
    .groupBy((k, order) -> order.getCategory())
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
    .aggregate(
        CategorySales::new,                      // initialiser: fresh accumulator per window
        (category, order, accumulator) ->        // aggregator: called for every record
            accumulator.update(category, order.getAmount(), order.getQuantity()),
        Materialized.as("category-sales-store")  // persisted in RocksDB, queryable via IQ
    );
```

The `CategorySales` accumulator object is a mutable POJO that is updated in-place for each record in the window. Kafka Streams stores it in RocksDB. When the window closes (1 minute passes), the final state is emitted downstream.

### 5.5 State Stores and Interactive Queries

State stores are Kafka Streams' embedded databases (backed by RocksDB by default). Unlike external databases, they live inside the application process and are replicated via internal Kafka changelog topics.

**Interactive Queries (IQ)** allow serving the state store contents over REST without any separate data store:

```java
// No database. No cache. Read directly from the RocksDB state store.
ReadOnlyWindowStore<String, CategorySales> store = kafkaStreams.store(
    StoreQueryParameters.fromNameAndType(
        "category-sales-store",
        QueryableStoreTypes.windowStore()
    )
);

// Fetch all category aggregations for the last 2 minutes
KeyValueIterator<Windowed<String>, CategorySales> iter =
    store.fetchAll(Instant.now().minus(Duration.ofMinutes(2)), Instant.now());
```

This is a unique capability of Kafka Streams — a microservice can serve its own analytics data directly from its local state, enabling a **query-where-you-compute** architecture.

---

## 6. ksqlDB — Deep Exploration

### 6.1 What ksqlDB Provides

ksqlDB is a **streaming SQL engine** built on top of Kafka Streams. It exposes the same Kafka Streams concepts (KStream → STREAM, KTable → TABLE) through SQL syntax, making stream processing accessible to engineers and analysts who know SQL.

ksqlDB runs as a **separate service** (its own Docker container) and communicates with Kafka like any other consumer/producer. It does not depend on the Spring Boot application — both layers operate independently on the same topics.

### 6.2 STREAM vs TABLE in ksqlDB

```sql
-- STREAM: maps to a Kafka topic, append-only, each SELECT row = one Kafka message
CREATE STREAM orders_stream (
    orderId VARCHAR, customerId VARCHAR, amount DOUBLE, ...
) WITH (KAFKA_TOPIC = 'orders', VALUE_FORMAT = 'JSON');

-- TABLE: materialised latest-per-key view, backed by an aggregate or compacted topic
-- Created by a persistent query (GROUP BY)
CREATE TABLE customer_spending AS
    SELECT customerId, SUM(amount) AS total_spent
    FROM orders_stream GROUP BY customerId EMIT CHANGES;
```

The `TABLE` is not a static snapshot — it is continuously updated as new events arrive on the `orders_stream`. Reading from it gives you the current state.

### 6.3 Push Queries vs Pull Queries

This distinction is unique to streaming SQL and has no direct equivalent in traditional databases:

**Push Query (`EMIT CHANGES`)** — a live subscription
```sql
-- Starts streaming results to the client; never terminates until cancelled
SELECT category, order_count, total_sales
FROM category_sales_1min
EMIT CHANGES;
```
- The client receives a new row every time the underlying TABLE updates
- This is ksqlDB's core value: live, push-based data delivery
- Suitable for: dashboards, alerting systems, real-time feeds

**Pull Query** — a point-in-time snapshot
```sql
-- Returns immediately with current state, like a traditional SELECT
SELECT category, order_count FROM category_sales_1min WHERE category = 'Electronics';
```
- Returns the current state and disconnects
- Reads from the materialised state store (RocksDB underneath)
- Suitable for: REST endpoint responses, one-off lookups

### 6.4 Windowing in ksqlDB

ksqlDB's windowing directly mirrors Kafka Streams' windowing:

```sql
-- TUMBLING: non-overlapping fixed buckets — ideal for per-minute/per-hour metrics
CREATE TABLE category_sales_1min AS
    SELECT category, COUNT(*) AS cnt, SUM(amount) AS total
    FROM orders_stream
    WINDOW TUMBLING (SIZE 1 MINUTE)
    GROUP BY category EMIT CHANGES;

-- HOPPING: overlapping windows — each event appears in multiple windows
-- Used for sliding averages, trending calculations
CREATE TABLE category_sales_hourly AS
    SELECT category, SUM(amount) AS total
    FROM orders_stream
    WINDOW HOPPING (SIZE 1 HOUR, ADVANCE BY 15 MINUTES)
    GROUP BY category EMIT CHANGES;

-- SESSION: variable-size, gap-based — ideal for user session analytics
-- Window closes when no events arrive for the gap duration
SELECT customerId, COUNT(*) AS actions
FROM orders_stream
WINDOW SESSION (30 MINUTES)
GROUP BY customerId EMIT CHANGES;
```

### 6.5 Persistent Queries — ksqlDB's "Always-On" Processing

When you run `CREATE TABLE AS SELECT` or `CREATE STREAM AS SELECT`, ksqlDB starts a **persistent query** — a background process that continuously reads from the source and writes results to a new Kafka topic.

```sql
-- This one statement starts a persistent background process:
-- reads from orders_stream → aggregates → writes to KSQL_CATEGORY_SALES_1MIN topic
CREATE TABLE IF NOT EXISTS category_sales_1min AS
    SELECT category, COUNT(*) AS order_count, SUM(amount) AS total_sales
    FROM orders_stream
    WINDOW TUMBLING (SIZE 1 MINUTE)
    GROUP BY category EMIT CHANGES;
```

Check running queries:
```sql
SHOW QUERIES;
-- Output shows query ID, state (RUNNING), and the source/sink topics
```

This is ksqlDB's equivalent of a Kafka Streams topology — but declared in SQL and managed by the ksqlDB server rather than compiled into application code.

### 6.6 ksqlDB REST API

ksqlDB exposes a REST API that the Spring Boot app (or any HTTP client) can call:

```bash
# Execute a KSQL statement
curl -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW TOPICS;", "streamsProperties": {}}'

# Push query (streaming HTTP response — server-sent events)
curl -X POST http://localhost:8088/query-stream \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"sql": "SELECT * FROM orders_stream EMIT CHANGES;",
       "properties": {"auto.offset.reset": "earliest"}}'

# Pull query (standard HTTP response)
curl -X POST http://localhost:8088/query \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SELECT * FROM category_sales_1min WHERE category = '\''Electronics'\'';",
       "streamsProperties": {}}'
```

---

## 7. Scripts and Commands for Streaming Integration

### 7.1 Kafka CLI — Direct Broker Interaction

```bash
# List topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Watch the orders topic in real time
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders --from-beginning \
  --property print.key=true

# Watch fraud alerts as they are generated
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic fraud-alerts --from-beginning

# Check consumer group lag for the Streams app
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group ecommerce-streams-app

# Manually produce a test order
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic orders --property parse.key=true --property key.separator="|"
# Then type: ord-001|{"orderId":"ord-001","customerId":"C001","productId":"P001","category":"Electronics","amount":999.99,"quantity":1,"status":"PENDING","timestamp":1712600000000}
```

### 7.2 ksqlDB CLI — Streaming SQL

```bash
# Open interactive CLI
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088

# Run all init statements from a file
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088 < ksql/init.sql
```

### 7.3 Spring Boot REST API

```bash
# Start continuous order simulation
curl -X POST "http://localhost:8090/api/orders/simulate/start?intervalMs=1500"

# Query live category aggregations (Kafka Streams Interactive Query)
curl -s "http://localhost:8090/api/analytics/category-sales" | python3 -m json.tool

# Stop simulation
curl -X POST "http://localhost:8090/api/orders/simulate/stop"
```

### 7.4 ksqlDB REST API — Programmatic Integration

```bash
# Push query — stream results (server-sent events, newline-delimited JSON)
curl -s -X POST http://localhost:8088/query-stream \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"sql":"SELECT category, total_sales FROM category_sales_1min EMIT CHANGES;",
       "properties":{"auto.offset.reset":"earliest"}}'
```

---

## 8. Conclusions and Inferences

### 8.1 What We Learned About Kafka Streams

1. **The stream/table duality** is the most important mental model. Understanding when to use KStream (append-only events) vs KTable (latest-per-key state) directly determines architecture correctness.

2. **Repartitioning is automatic but must be understood.** When `selectKey()` changes the message key (as in the order-enrichment join), Kafka Streams creates a new internal topic and repartitions the data. This adds latency but is unavoidable for correct join semantics.

3. **State stores eliminate external database dependency on the hot path.** The category-sales aggregation and customer-spending tracking are maintained in local RocksDB stores, not queried from a database. This makes the system horizontally scalable — each Kafka Streams instance owns a subset of partitions and the corresponding state.

4. **Interactive Queries enable a query-where-you-compute pattern.** The `AnalyticsController` serves live aggregations directly from in-process state stores without any additional caching layer (Redis, Memcached). This reduces operational complexity significantly.

5. **Time windowing is the key difference from batch.** Tumbling windows naturally replace "hourly batch jobs." The difference is that a tumbling window is computed continuously as events arrive, delivering the result in real time rather than hours later.

### 8.2 What We Learned About ksqlDB

1. **ksqlDB is Kafka Streams with a SQL interface.** Under the hood, every ksqlDB persistent query runs as a Kafka Streams topology. The SQL is compiled into a Kafka Streams program by the ksqlDB engine.

2. **Push queries are the core value proposition.** Traditional databases answer questions ("what is the count?"). ksqlDB streams answers as they change ("the count is now X, now Y, now Z"). This inversion of control — server pushes to client — is the fundamental shift in streaming SQL.

3. **The WINDOW clause in SQL makes time a first-class citizen.** Writing `WINDOW TUMBLING (SIZE 1 MINUTE)` in SQL is far more accessible than the equivalent Kafka Streams Java code, which requires understanding `TimeWindows`, `Materialized`, `WindowStore`, and window key semantics.

4. **Both layers complement each other perfectly.** Kafka Streams handles complex Java logic (custom serdes, multi-step topology, error handling). ksqlDB provides SQL access to the same data for analysts, dashboards, and rapid prototyping. Neither replaces the other in a production system.

### 8.3 Final Assessment

Kafka Streams and ksqlDB together represent a complete streaming platform. For the e-commerce use case:
- **Orders enriched with product data** in < 10ms (KStream-KTable join)
- **Fraud alerts generated** within the same processing cycle, before fulfilment begins
- **Category dashboards** updated every minute via tumbling window aggregation
- **Customer spending** tracked in real time with no ETL pipeline
- **SQL access** for business analysts via ksqlDB without writing Java

The combination demonstrates that stream processing is no longer a niche technology — it is a practical, production-ready approach to building low-latency, data-intensive applications. The Confluent reference (https://docs.confluent.io/platform/current/streams/overview.html) accurately describes Kafka Streams as enabling "event-driven microservices" — this project is a concrete example of that pattern applied to a real-world domain.
