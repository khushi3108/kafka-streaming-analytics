# End-to-End Demo Guide
## Paste → Run → See → Understand

> **This file is your single source of truth.**  
> Every command is paste-and-run. After each command you are told exactly what you will see and what it proves for the assignment.

---

## BEFORE YOU START — Check Your Machine

Open a terminal. Paste these one at a time.

```bash
java -version
```
✅ **You should see:** `java version "21.x.x"`  
❌ If not: install JDK 21 from https://adoptium.net

```bash
docker ps
```
✅ **You should see:** an empty table with column headers (Docker is running)  
❌ If you see "Cannot connect": start Colima with `colima start`

---

## STEP 1 — Start the Entire Kafka Stack

```bash
cd "/Users/Z009RSB/Desktop/stream assignment/Kafka"
docker compose up -d
```

This one command starts **7 containers** in the correct order automatically:

```
zookeeper  →  kafka  →  kafka-setup  →  schema-registry
                                     →  ksqldb-server  →  ksqldb-setup
                                     →  kafka-ui
```

Wait about 60 seconds then run:

```bash
docker compose ps
```

✅ **You should see something like this:**
```
NAME              STATUS
zookeeper         Up (healthy)
kafka             Up (healthy)
kafka-setup       Exited (0)        ← "Exited 0" = ran successfully and finished
schema-registry   Up (healthy)
ksqldb-server     Up (healthy)
ksqldb-setup      Exited (0)        ← ran init.sql successfully and finished
ksqldb-cli        Up
kafka-ui          Up
```

> **Assignment connection:** `kafka-setup` automatically created all 6 Kafka topics (orders, products, enriched-orders, fraud-alerts, category-sales, dead-letter), each with replication factor 3 and `min.insync.replicas=2` across the three brokers. `ksqldb-setup` automatically ran `ksql/init.sql` which created all ksqlDB STREAMs and TABLEs. No manual setup needed.

---

## STEP 2 — Verify the Topics Were Created

```bash
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
```

✅ **You should see:**
```
category-sales
dead-letter
enriched-orders
fraud-alerts
orders
products
```

> **Assignment connection:** These 6 topics are the backbone of the streaming pipeline. `orders` is where events enter. `enriched-orders`, `fraud-alerts` and `category-sales` are outputs written by the Kafka Streams topology (the Java code). `dead-letter` receives records that failed deserialization, with their raw bytes and failure metadata, instead of dropping them. This proves the infrastructure is wired correctly.

---

## STEP 3 — Verify ksqlDB Is Ready

```bash
curl -s http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql":"SHOW STREAMS;"}' | python3 -m json.tool
```

✅ **You should see** a JSON list containing:
```
orders_stream
enriched_orders_stream
fraud_alerts_stream
high_value_orders
electronics_orders
```

> **Assignment connection:** These are ksqlDB STREAMs — the "Kafka Streaming SQL" part of the assignment. Each one maps to a Kafka topic. You can now run SQL queries against live streaming data.

---

## STEP 4 — Build and Start the Spring Boot App

Open a **new terminal tab** (keep the first one for other commands).

```bash
cd "/Users/Z009RSB/Desktop/stream assignment/Kafka"
./gradlew bootRun
```

Wait about 20 seconds. Watch the logs scroll. You are looking for these 3 lines:

✅ **You should see in the logs:**
```
✅ Kafka Streams topology built successfully
📦 Initialising product catalog (20 products)...
✅ Product catalog initialisation complete.
```

> **What just happened:**
> 1. Spring Boot started and connected to Kafka
> 2. `ProductProducer` published 20 products to the `products` topic — this populates the **KTable** (a local lookup table in memory) that the streaming join will use
> 3. The **Kafka Streams topology** started — it is now continuously listening on the `orders` topic, ready to process every order in real time

Leave this terminal running. **All other commands go in a separate terminal.**

---

## STEP 5 — Check the App Is Running

In a new terminal:

```bash
curl -s http://localhost:8090/actuator/health
```

✅ **You should see:**
```json
{"status":"UP"}
```

```bash
curl -s http://localhost:8090/api/analytics/streams/status
```

✅ **You should see:**
```json
{"state":"RUNNING"}
```

> **Assignment connection:** `state: RUNNING` means the Kafka Streams topology is active and processing. It has connected to the broker, assigned partitions, and is ready to handle events.

---

## STEP 6 — Send Your First Order

This is the moment the pipeline wakes up. Paste this:

```bash
curl -s -X POST http://localhost:8090/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "C001",
    "productId":  "P001",
    "category":   "Electronics",
    "amount":     2499.99,
    "quantity":   1
  }'
```

✅ **You should see** the order JSON echoed back with an `orderId`.

Now **switch to the Spring Boot terminal** and look at the logs.

✅ **You should see in the logs:**
```
✅ Order sent [some-uuid] → partition=X offset=Y
```

> **What just happened — the full pipeline in one order:**
> 1. Your curl hit the REST API → `OrderProducer` published JSON to the `orders` topic
> 2. Kafka Streams picked it up instantly → joined with the `products` KTable → created an `EnrichedOrder` → published to `enriched-orders` topic
> 3. The `filter(amount > 500)` check ran → amount was $2499.99 so it fired → published to `fraud-alerts` topic
> 4. The category aggregation ran → `Electronics` window updated in the state store

---

## STEP 7 — Trigger a Fraud Alert

```bash
curl -s -X POST http://localhost:8090/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "C007",
    "productId":  "P001",
    "category":   "Electronics",
    "amount":     7499.97,
    "quantity":   3
  }'
```

**Switch to the Spring Boot terminal immediately.**

✅ **You should see this in the logs:**
```
🚨 FRAUD ALERT – orderId=xxxx amount=7499.97 customer=C007
```

> **Assignment connection:** This is **real-time fraud detection** — the Kafka Streams `filter()` operator evaluated this order in under 10ms and routed it to the `fraud-alerts` topic. There was no database query, no batch job, no waiting. This is the core value of stream processing over batch.

---

## STEP 8 — Generate Lots of Orders (Simulate Traffic)

```bash
curl -s -X POST "http://localhost:8090/api/orders/simulate?count=50"
```

✅ **You should see:**
```json
{"status":"done","ordersGenerated":50}
```

Then check the Spring Boot logs — you should see 50 order confirmations scroll past, plus several `🚨 FRAUD ALERT` lines (roughly 15% of orders are high-value by design).

---

## STEP 9 — Open Kafka UI and Watch the Topics

Open your browser: **http://localhost:9090**

Click **Topics** in the left menu. You should see all 5 topics.

**Click on `orders` → Messages tab**

✅ **You should see:** The 50+ JSON messages you just produced. Each one has the `orderId` as the key.

**Click on `enriched-orders` → Messages tab**

✅ **You should see:** The same orders but now each JSON has `productName` and `brand` fields added. These were not in the original order — Kafka Streams **joined** each order with the product KTable in real time.

**Click on `fraud-alerts` → Messages tab**

✅ **You should see:** Only the high-value orders (amount > $500). Kafka Streams **filtered** these out of the main stream automatically.

**Click on `category-sales` → Messages tab**

✅ **You should see:** Aggregation results — one message per category showing `orderCount`, `totalSales`, `avgOrderValue`. These are produced by the **tumbling window** aggregation.

> **Assignment connection:** You are seeing the complete Kafka Streams topology in action: Source → Join → Filter → Aggregate → Sink. Each topic is a stage in the pipeline.

---

## STEP 10 — Query the Live State Store (Interactive Queries)

```bash
curl -s "http://localhost:8090/api/analytics/category-sales" | python3 -m json.tool
```

✅ **You should see** something like:
```json
{
  "categories": {
    "Electronics": {
      "category": "Electronics",
      "orderCount": 12,
      "totalSales": 24876.45,
      "avgOrderValue": 2073.04,
      "maxOrderValue": 7499.97
    },
    "Clothing": { "orderCount": 8, "totalSales": 1439.92 },
    "Books":    { "orderCount": 5, "totalSales": 179.95 }
  }
}
```

```bash
curl -s "http://localhost:8090/api/analytics/customer-spending" | python3 -m json.tool
```

✅ **You should see** a ranked list of customers sorted by total spend.

> **Assignment connection:** This is **Kafka Streams Interactive Queries** — the REST endpoint reads directly from the local RocksDB state store inside the JVM. There is **no database**, no Redis, no external cache. The aggregation result lives inside the stream processor and is served directly. This is unique to Kafka Streams and does not exist in traditional messaging systems.

---

## STEP 11 — ksqlDB Interactive SQL Demo

Open the ksqlDB CLI:

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

You will see the ksqlDB prompt: `ksql>`

Now paste these one at a time and watch what happens:

### See what's available
```sql
SHOW STREAMS;
```
✅ **You should see:** orders_stream, enriched_orders_stream, fraud_alerts_stream, high_value_orders, electronics_orders

```sql
SHOW TABLES;
```
✅ **You should see:** category_sales_1min, category_sales_hourly, customer_spending, fraud_by_category

```sql
DESCRIBE orders_stream;
```
✅ **You should see:** the schema — all the columns (orderId, customerId, category, amount etc.)

> **Assignment connection:** `SHOW STREAMS` = KStreams. `SHOW TABLES` = KTables. This is the ksqlDB representation of the exact same stream/table duality that the Java Kafka Streams code uses. SQL syntax, same concepts.

---

### PUSH QUERY — Live streaming results

```sql
SELECT orderId, customerId, category, amount FROM orders_stream EMIT CHANGES LIMIT 5;
```

Now in another terminal, send an order:
```bash
curl -s -X POST "http://localhost:8090/api/orders/simulate?count=5"
```

✅ **Switch back to ksqlDB CLI** — you will see rows appear in the terminal as each order arrives.

> **Assignment connection:** `EMIT CHANGES` is the key concept of **Kafka Streaming SQL**. Unlike a normal SQL SELECT that returns a fixed result and stops, this query **keeps running** and pushes a new row every time a new Kafka message arrives. This is a **push query** — the server pushes data to you as it happens.

---

### Category aggregation — windowed SQL

```sql
SELECT category, order_count, total_sales, avg_order_value
FROM category_sales_1min
EMIT CHANGES;
```

In another terminal, generate more orders:
```bash
curl -s -X POST "http://localhost:8090/api/orders/simulate?count=20"
```

✅ **In ksqlDB CLI** you will see rows updating as orders arrive — the counts and totals change in real time.

> **Assignment connection:** This is `WINDOW TUMBLING (SIZE 1 MINUTE)` aggregation in SQL. Every order that arrives is immediately counted and summed. The window resets every minute. This is the ksqlDB equivalent of the Java `.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))` in the topology code.

---

### PULL QUERY — Point-in-time snapshot

```sql
SELECT category, order_count, total_sales FROM category_sales_1min WHERE category = 'Electronics';
```

✅ **You should see:** The current aggregated total for Electronics. The query **returns immediately** (no EMIT CHANGES) — like a regular SQL SELECT.

> **Assignment connection:** This is a **pull query** — the opposite of a push query. It reads from the materialised state store (RocksDB) and returns the current snapshot. Use push queries for dashboards/feeds; use pull queries for one-off lookups.

---

### Customer spending — running total

```sql
SELECT customerId, total_orders, total_spent FROM customer_spending EMIT CHANGES LIMIT 10;
```

✅ **You should see:** Each customer's running total, updating live.

> **Assignment connection:** This is an **unbounded aggregation** (no window) — it accumulates totals across all time. Every new order updates the TABLE. This shows how ksqlDB maintains a continuously updated materialised view.

---

### Fraud analytics

```sql
SELECT category, alert_count, flagged_amount FROM fraud_by_category;
```

✅ **You should see:** Which categories have the most high-value orders and total flagged amount.

> **Assignment connection:** This TABLE is fed from `fraud_alerts_stream` — itself fed from the `fraud-alerts` Kafka topic which was written by the Kafka Streams `filter()` step. This shows how ksqlDB and Kafka Streams **chain together** — Kafka Streams detects the fraud and routes it to a topic; ksqlDB aggregates the results in SQL.

---

**Exit ksqlDB CLI:**
```
exit
```

---

## STEP 12 — Start Continuous Simulation (for Demo/Recording)

This is the best mode for a recorded demo — orders flow in every 2 seconds continuously.

```bash
curl -s -X POST "http://localhost:8090/api/orders/simulate/start?intervalMs=2000"
```

✅ **You should see:** `{"status":"started"}`

Now open two windows side by side:
1. Kafka UI at http://localhost:9090 — watch messages accumulate in topics
2. ksqlDB CLI with `SELECT category, order_count, total_sales FROM category_sales_1min EMIT CHANGES;`

Watch the numbers update live every few seconds.

**Stop the simulation when done:**
```bash
curl -s -X POST "http://localhost:8090/api/orders/simulate/stop"
```

---

## STEP 13 — Stop Everything

```bash
# Spring Boot: press Ctrl+C in the terminal running ./gradlew bootRun

# Stop Docker
docker compose down

# Full clean (wipes all topic data — use before a fresh demo)
docker compose down -v
```

---

## The Complete Flow — How It All Connects

```
YOU (curl command)
      │
      │  POST /api/orders  {"amount": 2499.99 ...}
      ▼
Spring Boot REST API  (:8090)
      │
      │  OrderProducer.send("orders", orderId, jsonString)
      ▼
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  KAFKA BROKER  (topics)
  [orders topic]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
      │
      │  Kafka Streams reads every new message instantly
      ▼
Kafka Streams Topology (inside Spring Boot JVM)

  Step 1: Re-key by productId
  Step 2: leftJoin(products KTable)  →  adds productName, brand
  Step 3: filter(amount > 500)       →  writes to fraud-alerts topic
  Step 4: groupBy(category)          →  windowed aggregate → category-sales topic
  Step 5: groupBy(customerId)        →  running total → state store (RocksDB)

      │ writes to topics
      ▼
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  KAFKA BROKER  (output topics)
  [enriched-orders]  [fraud-alerts]  [category-sales]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
      │
      │  ksqlDB reads the SAME topics independently
      ▼
ksqlDB (separate process, :8088)
  STREAM orders_stream        ← reads orders topic in SQL
  TABLE  category_sales_1min  ← TUMBLING window aggregation in SQL
  TABLE  customer_spending     ← running total in SQL
  STREAM high_value_orders     ← filter in SQL

      │
      ▼  You query via:
  ksqlDB CLI:       SELECT ... EMIT CHANGES;    (push = live feed)
  ksqlDB CLI:       SELECT ... WHERE ...;       (pull = snapshot)
  REST API:         GET /api/analytics/*        (reads from RocksDB state store)
  Kafka UI:         http://localhost:9090        (see raw messages in topics)
```

---

## What Each Part Proves for the Assignment

| What you ran | What it proves |
|---|---|
| `docker compose up -d` | The full Confluent stack (Kafka, ksqlDB, Schema Registry) runs containerised |
| Topics auto-created | Kafka topics are the backbone — partitioned, replicated, ordered logs |
| `./gradlew bootRun` | Kafka Streams runs as a library inside a Java app — no separate cluster |
| `POST /api/orders` | Events enter the system as JSON messages on a Kafka topic |
| Kafka UI enriched-orders | KStream-KTable join — order enriched with product details in-flight |
| Kafka UI fraud-alerts | Stateless filter — sub-second routing based on a condition |
| Kafka UI category-sales | Windowed aggregation — rolling 1-minute totals |
| `GET /api/analytics/*` | Interactive Queries — read live state store with NO database |
| `EMIT CHANGES` in ksqlDB | Push query — server sends rows as they arrive, never stops |
| `SELECT ... WHERE` in ksqlDB | Pull query — snapshot of current materialised state |
| `WINDOW TUMBLING` in ksqlDB | Same windowing as Java code but written in SQL |
| `SHOW QUERIES` in ksqlDB | Persistent queries — background processes running SQL on streams |

