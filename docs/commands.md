# End-to-End Demo Guide
## Paste → Run → See → Understand

> Every command below is paste-and-run. After each one you are told what you should see and
> what it demonstrates. Design rationale and known limitations are in the
> [root README](../README.md); this file is purely operational.

---

## BEFORE YOU START — Check Your Machine

```bash
java -version
```
✅ **You should see:** `openjdk version "21.x.x"` (or another JDK 21 build)
❌ If not: install JDK 21 from https://adoptium.net and set `JAVA_HOME`:
```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)   # macOS
```
The Gradle build declares a Java 21 toolchain, so Gradle will try to resolve or provision
one itself; `JAVA_HOME` is the fallback when it cannot.

```bash
docker ps
```
✅ **You should see:** an empty table with column headers (Docker is running)
❌ If you see "Cannot connect": start Docker Desktop, or `colima start`.

---

## STEP 1 — Start the Entire Kafka Stack

There are two ways to run this. Pick one and stay with it.

### Option A — everything in Docker (recommended for a demo)

```bash
cd /path/to/kafka-streaming-analytics
docker compose up -d
```

This starts the infrastructure **and two application instances** built from the
`Dockerfile`, plus the Prometheus/Grafana observability stack. The first run builds the app
image and takes a few minutes.

```
zookeeper ─┬─► kafka-1 ─┐                        ┌─► schema-registry ─► ksqldb-server ─┬─► ksqldb-setup
           ├─► kafka-2 ─┼─► kafka-setup ─────────┼─► kafka-ui                          └─► ksqldb-cli
           └─► kafka-3 ─┘                        ├─► app-1  (:8090)
                       └─► kafka-exporter        └─► app-2  (:8091)
                              └─► prometheus (:9091) ─► grafana (:3000)
```

Wait about 60 seconds (the app containers have a 90-second health-check start period), then:

```bash
docker compose ps
```

✅ **You should see something like:**
```
NAME              STATUS
zookeeper         Up (healthy)
kafka-1           Up (healthy)
kafka-2           Up (healthy)
kafka-3           Up (healthy)
kafka-setup       Exited (0)        ← ran successfully and finished
schema-registry   Up (healthy)
ksqldb-server     Up (healthy)
ksqldb-setup      Exited (0)        ← ran init.sql successfully and finished
ksqldb-cli        Up
kafka-ui          Up
app-1             Up (healthy)      ← healthy means queryable:true, i.e. Streams is RUNNING
app-2             Up (healthy)
kafka-exporter    Up
prometheus        Up
grafana           Up
```

With Option A, **skip STEP 4** (the app is already running) and read the app logs with
`docker compose logs -f app-1` wherever the steps below say "switch to the Spring Boot
terminal".

### Option B — infrastructure in Docker, app on the host

Better if you are editing the topology, or if the Docker Desktop VM cannot spare the memory
for the two app containers. Naming the infrastructure services explicitly leaves 8090/8091
free:

```bash
cd /path/to/kafka-streaming-analytics
docker compose up -d kafka-1 kafka-2 kafka-3 kafka-setup \
                     schema-registry ksqldb-server ksqldb-setup kafka-ui
```

> **Assignment connection:** `kafka-setup` created all six topics (`orders`, `products`,
> `enriched-orders`, `fraud-alerts`, `category-sales`, `dead-letter`), each with
> replication factor 3 and `min.insync.replicas=2` across the three brokers. Three brokers
> is not decoration: the Streams app runs `EXACTLY_ONCE_V2`, which is implemented with
> Kafka transactions whose state lives in `__transaction_state`. At RF=1 losing the single
> broker would lose the transaction log — a guarantee with nothing durable underneath it.
> `ksqldb-setup` ran `ksql/init.sql`, which created every ksqlDB STREAM and TABLE. No
> manual setup is needed.

---

## STEP 2 — Verify the Topics Were Created

```bash
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
```

✅ **You should see** (plus ksqlDB's own internal topics):
```
category-sales
dead-letter
enriched-orders
fraud-alerts
orders
products
```

Check the durability settings actually landed:

```bash
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic orders
```

✅ **You should see** `ReplicationFactor: 3` and `Configs: min.insync.replicas=2`.

> **Assignment connection:** `orders` is where events enter. `enriched-orders`,
> `fraud-alerts` and `category-sales` are outputs written by the Kafka Streams topology.
> `dead-letter` receives records that failed deserialization — raw bytes plus failure
> metadata — instead of them being dropped with a log line.

---

## STEP 3 — Verify ksqlDB Is Ready

```bash
curl -s http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql":"SHOW STREAMS;"}' | python3 -m json.tool
```

✅ **You should see** a JSON list containing:
```
ORDERS_STREAM
ENRICHED_ORDERS_STREAM
FRAUD_ALERTS_STREAM
HIGH_VALUE_ORDERS
ELECTRONICS_ORDERS
```

> **Assignment connection:** These are ksqlDB STREAMs — the streaming-SQL half of the
> assignment. Each maps to a Kafka topic. ksqlDB reads those topics as an entirely
> independent consumer; it is not downstream of the Java application.

---

## STEP 4 — Build and Start the Spring Boot App  *(Option B only)*

If you used Option A, the app is already running as `app-1` and `app-2` — skip to STEP 5.

Open a **new terminal tab**.

```bash
cd /path/to/kafka-streaming-analytics
./gradlew bootRun
```

Wait ~20 seconds. Look for these lines:

```
✅ Kafka Streams topology built successfully
📦 Initialising product catalog (20 products)...
✅ Product catalog initialisation complete.
Interactive Queries: this instance advertises itself as localhost:8090
```

> **What just happened:**
> 1. Spring Boot started and connected to the three brokers.
> 2. `ProductProducer` published 20 products to the compacted `products` topic, which
>    materialises the `products-store` KTable that the enrichment join reads.
> 3. The topology started, running under `EXACTLY_ONCE_V2` with a custom
>    `OrderTimestampExtractor`, so windows are assigned from the order's own `timestamp`
>    field rather than the broker's append time.

> ⚠️ The banner the app prints on startup says port **8080**. It is wrong.
> `application.yml` sets `server.port: 8090` — use 8090 everywhere.

Leave this terminal running.

---

## STEP 5 — Check the App Is Running

```bash
curl -s http://localhost:8090/actuator/health
```
✅ `{"status":"UP", ...}`

```bash
curl -s http://localhost:8090/api/analytics/streams/status | python3 -m json.tool
```

✅ **You should see** something like:
```json
{
  "state": "RUNNING",
  "queryable": true,
  "localHost": "localhost:8090",
  "instances": [
    {
      "host": "localhost:8090",
      "self": true,
      "activeStores": ["category-sales-store", "customer-session-store",
                       "customer-spending-store", "customer-velocity-store",
                       "products-store"],
      "topicPartitions": ["orders-0", "orders-1", "orders-2", "..."]
    }
  ]
}
```

> **Assignment connection:** `state: RUNNING` means the topology has joined the consumer
> group, been assigned partitions and restored its state stores. This endpoint deliberately
> returns `200` in every state, including `REBALANCING` — it is what you call to find out
> why the other endpoints are returning `503`.

---

## STEP 6 — Send Your First Order

```bash
curl -s -X POST http://localhost:8090/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "C001",
    "productId":  "P001",
    "category":   "Electronics",
    "amount":     2499.99,
    "quantity":   1
  }' | python3 -m json.tool
```

✅ **You should see** the order echoed back with a generated `orderId` and `timestamp`
(HTTP 202 Accepted).

Switch to the Spring Boot terminal.

✅ **You should see:**
```
✅ Order sent [some-uuid] → partition=X offset=Y
🚨 HIGH_VALUE – orderId=some-uuid amount=2499.99 customer=C001
```

> **What just happened — the pipeline for one order:**
> 1. `OrderProducer` published the JSON to `orders` with `acks=all` and idempotence on.
> 2. The topology re-keyed by `productId`, left-joined the `products` KTable, and wrote an
>    `EnrichedOrder` (now carrying `productName` and `brand`) to `enriched-orders`.
> 3. The stream was re-keyed and repartitioned by `customerId`, and the four anomaly
>    signals evaluated it. `HIGH_VALUE` fired because 2499.99 exceeds the 500 threshold.
> 4. The category aggregation updated the `Electronics` window in `category-sales-store`.
>    Nothing was written to the `category-sales` topic yet — suppression holds the window
>    until it closes.

---

## STEP 7 — Watch the Anomaly Signals

The four signals answer different questions, and one order will not trigger all of them.

### 7a. `HIGH_VALUE` — single order over the absolute threshold

```bash
curl -s -X POST http://localhost:8090/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"C007","productId":"P001","category":"Electronics","amount":7499.97,"quantity":3}'
```

✅ **In the app log:** `🚨 HIGH_VALUE – orderId=... amount=7499.97 customer=C007`

### 7b. `VELOCITY` — more than 3 orders from one customer in the trailing 5 minutes

```bash
for i in 1 2 3 4 5; do
  curl -s -X POST http://localhost:8090/api/orders \
    -H "Content-Type: application/json" \
    -d '{"customerId":"C009","productId":"P012","category":"Books","amount":35.99,"quantity":1}' \
    > /dev/null
done
```

✅ **In the app log:** `🚨 VELOCITY – customer=C009 orders=4 total=... window=[...]`

Note none of these orders is individually suspicious — $35.99 each. That is the point: a
constant threshold cannot see a rate.

### 7c. `SESSION_BURST` — a whole session with ≥ 8 orders, or over $3000

```bash
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:8090/api/orders \
    -H "Content-Type: application/json" \
    -d '{"customerId":"C011","productId":"P015","category":"Food","amount":24.99,"quantity":1}' \
    > /dev/null
done
```

The session signal is **suppressed until the window closes**, so the alert appears only
after C011 has been quiet for the 5-minute inactivity gap plus 1 minute of grace — and
because that is measured in *stream time*, more orders have to arrive to advance the clock.
Leave the continuous simulator (STEP 12) running if you want to see it fire.

### 7d. `BASELINE_DEVIATION` — abnormal *for this customer*

```bash
# Establish a baseline: 6 small orders (the signal needs >= 5 prior orders)
for i in $(seq 1 6); do
  curl -s -X POST http://localhost:8090/api/orders \
    -H "Content-Type: application/json" \
    -d '{"customerId":"C013","productId":"P009","category":"Clothing","amount":69.99,"quantity":1}' \
    > /dev/null
done

# Now one that is far above their own average — but below the 500 HIGH_VALUE threshold
curl -s -X POST http://localhost:8090/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"C013","productId":"P010","category":"Clothing","amount":449.99,"quantity":1}'
```

✅ **In the app log:**
`🚨 BASELINE_DEVIATION – customer=C013 orderId=... amount=449.99 priorAvg=69.99 priorOrders=6`

> **Assignment connection:** $449.99 never trips the absolute threshold, and six orders in
> a few seconds may or may not trip velocity — but it is 6× this customer's own average,
> which is the question a constant cannot ask. All four signals write to the same
> `fraud-alerts` topic; `alertType` says which one fired.

---

## STEP 8 — Generate Traffic

```bash
curl -s -X POST "http://localhost:8090/api/orders/simulate?count=50" | python3 -m json.tool
```

✅ `{"status":"done","ordersGenerated":50}`

The simulator draws from a 20-product catalog across 15 customer IDs, with a 15% chance of
a multi-unit "surge" order. Several of the catalog prices exceed $500 on their own, so
expect a mix of `HIGH_VALUE` and, as customers accumulate history, `VELOCITY` and
`BASELINE_DEVIATION` alerts in the log.

---

## STEP 9 — Browse the Topics in Kafka UI

Open **http://localhost:9090** → **Topics**. You should see the six application topics.

| Topic → Messages tab | What you should see |
|---|---|
| `orders` | The raw JSON you produced, keyed by `orderId` |
| `enriched-orders` | The same orders, now with `productName` and `brand` — added by the KStream–KTable join in flight |
| `fraud-alerts` | Alerts keyed by **`customerId`**, each with an `alertType` of `HIGH_VALUE`, `VELOCITY`, `SESSION_BURST` or `BASELINE_DEVIATION`, plus a deterministic `alertId` and a `reason` |
| `category-sales` | **One message per closed window per category** — not one per order. Suppression collapses the window to a single final result |
| `dead-letter` | Empty, unless you have deliberately produced malformed JSON to `orders` |

Two things worth noticing:

- The aggregate alerts (`VELOCITY`, `SESSION_BURST`) have `orderId`, `productId` and
  `category` set to `null`. They describe a window of behaviour, not one order; inventing a
  representative `orderId` would be misleading. Their `amount` is the window total and
  their `reason` carries the order count.
- `category-sales` messages only appear once a window has closed (1 minute + 30 seconds of
  grace, measured in event time). If the stream goes idle, the last window may not emit at
  all until more records arrive to advance stream time.

### Produce a poison pill to exercise the dead-letter topic

```bash
docker exec -i kafka-1 kafka-console-producer \
  --bootstrap-server localhost:9092 --topic orders <<< 'this is not json'
```

Then look at `dead-letter` in Kafka UI, or:

```bash
docker exec -it kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic dead-letter --from-beginning \
  --property print.headers=true --max-messages 1
```

✅ **You should see** the original bytes plus headers `dlq.original.topic`,
`dlq.original.partition`, `dlq.original.offset`, `dlq.exception.class`,
`dlq.exception.message`.

> **Assignment connection:** the old handler was `LogAndContinueExceptionHandler`, which
> emitted one WARN line and discarded the bytes — nothing to replay, nothing to audit.
> Note the honest caveat: the DLQ producer is **not** part of the Streams transaction, so
> DLQ writes are at-least-once. Dedupe on
> `(dlq.original.topic, dlq.original.partition, dlq.original.offset)`.

---

## STEP 10 — Query the Live State Store (Interactive Queries)

```bash
curl -s "http://localhost:8090/api/analytics/category-sales?from=1970-01-01T00:00:00Z" \
  | python3 -m json.tool
```

✅ **You should see** a list of windows — one entry per `(category, window)`:
```json
{
  "from": "1970-01-01T00:00:00Z",
  "to": "2026-09-04T12:03:00Z",
  "fetchedAt": "2026-09-04T12:03:00.412Z",
  "partial": false,
  "hostsQueried": ["localhost:8090"],
  "failures": [],
  "localOnly": false,
  "windowCount": 4,
  "windows": [
    {
      "category": "Electronics",
      "windowStart": "2026-09-04T12:01:00Z",
      "windowEnd":   "2026-09-04T12:02:00Z",
      "windowStartMs": 1788609660000,
      "windowEndMs":   1788609720000,
      "orderCount": 12,
      "totalSales": "24876.45",
      "totalQuantity": 17,
      "avgOrderValue": "2073.04",
      "maxOrderValue": "7499.97",
      "minOrderValue": "349.99"
    }
  ]
}
```

> **Why `from=1970-01-01T00:00:00Z`?** The window store is keyed by **event time** — the
> order's own `timestamp` field — and `from`/`to` are matched against window **start**
> times. The `windowMinutes` convenience parameter still anchors on `Instant.now()`, which
> agrees with event time only while the app is live and caught up. After a restart,
> `auto.offset.reset=earliest` replays the topic and fills the store with windows whose
> event times are in the past, so a wall-clock range can return an empty list while the app
> is working perfectly. Passing an explicit `from` removes the ambiguity.

> **Windows are not collapsed.** An earlier version returned a `Map` keyed by category and
> resolved collisions by keeping whichever entry had more orders — so with 1-minute windows
> and a 2-minute default span, every category active in both windows silently lost one of
> them. Distinct windows are distinct facts.

```bash
curl -s "http://localhost:8090/api/analytics/customer-spending" | python3 -m json.tool
```

✅ A ranked list of customers by `totalSpent`, with `partial`, `hostsQueried` and
`failures` alongside.

```bash
curl -s "http://localhost:8090/api/analytics/customer-spending/C001" | python3 -m json.tool
```

✅ One customer, with `servedBy` and `servedLocally` naming the instance that answered. An
unknown customer is `200` with `"found": false` — the key is valid, they simply have not
ordered.

> **Assignment connection:** these endpoints read the RocksDB state stores the topology
> already maintains — no Redis, no Postgres, no cache-invalidation window. The trade-off is
> that read availability is coupled to stream-thread health: during a rebalance or a state
> restore these endpoints return `503` + `Retry-After` rather than a stale or partial answer
> presented as complete. See the root README's design-decisions section.

---

## STEP 11 — Multi-Instance Interactive Queries

This is the part that only exists with more than one instance running.

**Option A:** you already have two — `app-1` on :8090 and `app-2` on :8091. Skip straight
to the status check below.

**Option B:** start a second JVM in a third terminal:

```bash
cd /path/to/kafka-streaming-analytics
./gradlew bootRun --args='--server.port=8091'
```

Nothing else changes: the advertised `application.server` address defaults to
`localhost:${server.port}`, and the RocksDB state directory leaf is derived from
`host-port` so the second JVM does not collide with the first on RocksDB's exclusive
directory lock. (The compose containers set `APP_STREAMS_APPLICATION_SERVER` to `app-1:8090`
/ `app-2:8091` instead, because inside a container `localhost` is that container.)

Wait for the rebalance to settle, then:

```bash
curl -s http://localhost:8090/api/analytics/streams/status | python3 -m json.tool
```

✅ **You should now see two entries in `instances`**, each with its own
`topicPartitions` list. The partitions are split between them.

Now compare a local read against a cluster-wide one:

```bash
# This instance's partitions only
curl -s "http://localhost:8090/api/analytics/customer-spending?local=true" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print("local  :8090 ->", d["totalCustomers"], "customers")'

curl -s "http://localhost:8091/api/analytics/customer-spending?local=true" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print("local  :8091 ->", d["totalCustomers"], "customers")'

# Fan-out across both
curl -s "http://localhost:8090/api/analytics/customer-spending" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print("fanout :8090 ->", d["totalCustomers"], "customers, partial =", d["partial"], d["hostsQueried"])'
```

✅ The two `local=true` counts should sum to the fan-out count.

> **Assignment connection — the bug this fixes.** A Kafka Streams state store is sharded by
> partition; `store.all()` iterates only the partitions *this* instance owns. There is no
> cluster-wide iterator. The previous controller called it directly and returned the result
> as the whole answer, so with two instances `GET /customer-spending` returned roughly half
> the customers **with HTTP 200 and no indication anything was missing**. A silently wrong
> answer is worse than a crash, because a crash is detectable.

### See a partial result

Kill the second instance (Ctrl+C in its terminal) and immediately re-run the fan-out
query, before the group finishes rebalancing:

```bash
curl -s "http://localhost:8090/api/analytics/customer-spending" | python3 -m json.tool | head -20
```

You will see one of:

- `"partial": true` with a `failures` entry naming `localhost:8091` and a reason of
  `RPC_FAILED` or `TIMEOUT` — the answer is real but incomplete, and says so;
- HTTP `503` with `"code": "STREAMS_NOT_READY"` and a `Retry-After` header, if the group
  is mid-rebalance and partition ownership is unknown.

**Branch on `partial`.** Both outcomes are correct behaviour. What is not acceptable — and
what this replaced — is a `200` that looks complete and is not.

---

## STEP 12 — ksqlDB Interactive SQL

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

### See what exists
```sql
SHOW STREAMS;
SHOW TABLES;
SHOW QUERIES;
DESCRIBE orders_stream;
```

✅ Streams: `ORDERS_STREAM`, `ENRICHED_ORDERS_STREAM`, `FRAUD_ALERTS_STREAM`,
`HIGH_VALUE_ORDERS`, `ELECTRONICS_ORDERS`.
✅ Tables: `CATEGORY_SALES_1MIN`, `CATEGORY_SALES_HOURLY`, `CUSTOMER_SPENDING`,
`FRAUD_BY_CATEGORY`.

> **Assignment connection:** `SHOW STREAMS` = KStreams, `SHOW TABLES` = KTables. Same
> stream/table duality as the Java DSL, expressed in SQL.

### PUSH query — a live subscription
```sql
SELECT orderId, customerId, category, amount FROM orders_stream EMIT CHANGES LIMIT 5;
```
In another terminal:
```bash
curl -s -X POST "http://localhost:8090/api/orders/simulate?count=5"
```
✅ Rows appear in the CLI as each order arrives.

> **Assignment connection:** `EMIT CHANGES` is the core of streaming SQL. A normal SELECT
> returns a fixed result and stops; this one never terminates and pushes a row every time a
> new record arrives.

### Windowed aggregation in SQL
```sql
SELECT category, order_count, total_sales, avg_order_value
FROM category_sales_1min
EMIT CHANGES;
```
```bash
curl -s -X POST "http://localhost:8090/api/orders/simulate?count=20"
```
✅ Counts and totals update as orders arrive.

> **Assignment connection:** this is `WINDOW TUMBLING (SIZE 1 MINUTE)` — the SQL equivalent
> of `TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(30))` in
> `OrderStreamTopology`. Both engines bucket on the same clock: ksqlDB because
> `init.sql` declares `TIMESTAMP='timestamp'`, and Java because `OrderTimestampExtractor`
> reads the same payload field. Before that extractor existed the Java side used the
> broker's append time and the two engines disagreed about which minute an order belonged
> to.
>
> They still differ in two ways: the Java side has a 30-second grace period and emits one
> suppressed result per window, and ksqlDB declares `amount` as `DOUBLE` while the Java
> side uses `BigDecimal`. Expect close agreement, not exact.

### PULL query — a point-in-time snapshot
```sql
SELECT category, order_count, total_sales
FROM category_sales_1min
WHERE category = 'Electronics';
```
✅ Returns immediately and disconnects, reading the materialised state.

### Customer spending
```sql
SELECT customerId, total_orders, total_spent FROM customer_spending EMIT CHANGES LIMIT 10;
```

### Alerts
```sql
SELECT alertId, customerId, alertType, severity, amount
FROM fraud_alerts_stream
EMIT CHANGES;
```
✅ You will see all four `alertType` values flow past.

```sql
SELECT category, alert_count, flagged_amount FROM fraud_by_category EMIT CHANGES;
```

> ⚠️ `fraud_by_category` groups by `category`, and `VELOCITY` / `SESSION_BURST` alerts
> deliberately leave `category` null — ksqlDB drops null grouping keys. This table
> therefore counts only `HIGH_VALUE` and `BASELINE_DEVIATION` alerts. `fraud_alerts_stream`
> also has no `reason` column, so the text justifying an aggregate alert is invisible in
> SQL. Both are stale-schema issues in `ksql/init.sql`, not in the Java topology.

> ⚠️ `ksql/queries.sql` contains several statements that will not run: snake_case column
> names (`order_id`, `customer_id`, `product_name`) that `init.sql` never declares, and a
> table `customer_spending_total` that does not exist. Use the queries above.

**Exit:** `exit`

---

## STEP 13 — Continuous Simulation

The best mode for a recorded demo — a steady order rate, which also keeps stream time
advancing so suppressed windows actually close.

```bash
curl -s -X POST "http://localhost:8090/api/orders/simulate/start?intervalMs=2000"
```
✅ `{"status":"started","intervalMs":2000, ...}`

Put two windows side by side:
1. Kafka UI at http://localhost:9090, watching `category-sales` — one message per window.
2. ksqlDB CLI running
   `SELECT category, order_count, total_sales FROM category_sales_1min EMIT CHANGES;`

Stop when done:
```bash
curl -s -X POST "http://localhost:8090/api/orders/simulate/stop"
```

---

## STEP 14 — Useful Kafka CLI Commands

```bash
# Consumer group lag for the Streams app
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --group ecommerce-streams-app

# Watch committed records only (the topology runs EXACTLY_ONCE_V2)
docker exec -it kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic fraud-alerts --from-beginning \
  --isolation-level read_committed

# List internal Streams topics (repartition + changelog)
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list \
  | grep ecommerce-streams-app

# Produce a hand-written order
docker exec -it kafka-1 kafka-console-producer \
  --bootstrap-server localhost:9092 --topic orders \
  --property parse.key=true --property key.separator="|"
# then type:
# ord-001|{"orderId":"ord-001","customerId":"C001","productId":"P001","category":"Electronics","amount":999.99,"quantity":1,"status":"PENDING","timestamp":1788609660000}
```

Note the `timestamp` field in that last payload: it is the event time the topology will
window on. Setting it to a value in the past is the easiest way to see grace periods and
late-record handling in action.

---

## STEP 15 — Stop Everything

```bash
# Spring Boot: Ctrl+C in each bootRun terminal

docker compose down       # stop containers, keep topic data
docker compose down -v    # wipe volumes for a clean slate
```

To force a full state restore from the changelog topics on the next start — a good way to
see the `503` / `partial` behaviour — delete the RocksDB directories:

```bash
rm -rf /tmp/kafka-streams/ecommerce
```

---

## The Complete Flow

```
YOU (curl)
  │  POST /api/orders   {"amount": 2499.99, "timestamp": ...}
  ▼
Spring Boot REST API (:8090)
  │  OrderProducer → acks=all, idempotent
  ▼
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  KAFKA · 3 brokers · RF=3 · min.insync.replicas=2
  [orders]                                    [products, compacted]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  │                                             │
  ▼                                             ▼
Kafka Streams topology (in the Spring Boot JVM, EXACTLY_ONCE_V2)
  event time from the payload's `timestamp` field

  1. rekey by productId → leftJoin(products KTable) → EnrichedOrder → [enriched-orders]
  2. groupBy(category) → tumbling 1m + 30s grace → suppress until close
                                                 → [category-sales] + category-sales-store
  3. repartition by customerId (one shared internal topic), then:
       aggregate lifetime spend           → customer-spending-store
       HIGH_VALUE          (per record)   ─┐
       VELOCITY            (hopping 5m/1m) ├─► merge → [fraud-alerts], key = customerId
       SESSION_BURST       (session 5m)    │
       BASELINE_DEVIATION  (stream ⋈ table)┘
  deserialization failure                  → [dead-letter] (at-least-once, separate producer)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  │                                             │
  ▼                                             ▼
ksqlDB (:8088, independent consumer)      REST /api/analytics
  STREAM orders_stream                      AnalyticsController
  TABLE  category_sales_1min                  → InteractiveQueryService
  TABLE  category_sales_hourly                    keyed lookup → the one owning instance
  TABLE  customer_spending                        unkeyed scan → parallel fan-out to peers
  STREAM high_value_orders                        merge, and report partial + failures
  TABLE  fraud_by_category                    reads RocksDB directly — no database
```

---

## What Each Part Demonstrates

| What you ran | What it demonstrates |
|---|---|
| `docker compose up -d` | The Confluent stack, three brokers, running containerised with automatic topic and ksqlDB bootstrap |
| Topics at RF=3 / minISR=2 | Durability that the `EXACTLY_ONCE_V2` guarantee actually rests on |
| `./gradlew bootRun` | Kafka Streams as a library inside a Java app — no separate processing cluster |
| `POST /api/orders` | Events entering the system as JSON on a Kafka topic |
| `enriched-orders` in Kafka UI | KStream–KTable join — enrichment in flight, no database lookup |
| Four `alertType` values | Stateless filter, hopping window, session window and stream–table join, all in one topology |
| One message per window on `category-sales` | Suppression — one final result per window instead of one per record |
| Explicit `from`/`to` on `/category-sales` | Event-time semantics made visible in the API |
| `GET /api/analytics/*` | Interactive Queries — serving live state with no database |
| `local=true` vs fan-out counts | State stores are sharded by partition; a cluster-wide answer requires querying every instance |
| `partial: true` / `503` | Incomplete answers reported as incomplete rather than returned as complete |
| Poison pill → `dead-letter` | Failed records retained with metadata instead of dropped |
| `EMIT CHANGES` in ksqlDB | Push query — the server pushes rows as they arrive |
| `SELECT ... WHERE` in ksqlDB | Pull query — a snapshot of materialised state |
| `WINDOW TUMBLING` in ksqlDB | The same windowing as the Java code, on the same event-time clock |
