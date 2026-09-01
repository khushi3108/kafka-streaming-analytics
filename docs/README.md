# Real-Time E-Commerce Order Analytics with Kafka Streams & ksqlDB

## Use Case

This project demonstrates a **real-time e-commerce order analytics platform** using Apache Kafka Streams and ksqlDB. When a customer places an order, it flows through a streaming pipeline that:

- **Enriches** orders with product details (KStream–KTable join)
- **Detects fraud** for high-value orders in real time (stream filter)
- **Aggregates** sales per category in 1-minute tumbling windows
- **Tracks** customer spending via interactive queries
- **Enables SQL** analytics through ksqlDB (push + pull queries)

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Java | 17+ | https://adoptium.net |
| Docker Desktop | latest | https://docker.com |
| Gradle | 8.5 (via wrapper) | bundled |
| curl | any | built-in macOS |

---

## Quick Start

### 1. Clone / open the project
```bash
cd "stream assignment/Kafka"
```

### 2. Run setup (downloads wrapper, pulls images)
```bash
bash scripts/setup.sh
```

### 3. Start infrastructure
```bash
docker compose up -d
```
Wait ~30 seconds for all services to become healthy.

Verify:
```bash
docker compose ps          # all services should show "Up"
curl http://localhost:8088  # ksqlDB should respond
curl http://localhost:9090  # Kafka UI should load
```

### 4. Build & run Spring Boot application
```bash
./gradlew bootRun
```
Or build a JAR:
```bash
./gradlew build
java -jar build/libs/ecommerce-kafka-streaming-0.0.1-SNAPSHOT.jar
```

### 5. Run the demo
```bash
bash scripts/demo.sh
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                    Docker Compose                                 │
│  ┌──────────┐  ┌──────────┐  ┌───────────────┐  ┌────────────┐  │
│  │Zookeeper │  │  Kafka   │  │ Schema        │  │  ksqlDB    │  │
│  │ :2181    │  │  :9092   │  │ Registry:8081 │  │  :8088     │  │
│  └──────────┘  └────┬─────┘  └───────────────┘  └────────────┘  │
│                     │                                ↑           │
└─────────────────────┼────────────────────────────────┼───────────┘
                      │  Kafka Topics                  │ SQL Queries
         ┌────────────┴──────────────────────────┐     │
         │    orders | products | enriched-orders │◄────┘
         │    fraud-alerts | category-sales       │
         └───────────────┬───────────────────────┘
                         │  Kafka Streams
         ┌───────────────▼───────────────────────┐
         │         Spring Boot App :8080          │
         │  ┌─────────────────────────────────┐   │
         │  │   OrderStreamTopology           │   │
         │  │   1. Join (orders + products)   │   │
         │  │   2. Filter (amount > $500)     │   │
         │  │   3. Aggregate (by category)    │   │
         │  │   4. Track (customer spending)  │   │
         │  └─────────────────────────────────┘   │
         │  REST API: /api/orders, /api/analytics  │
         └────────────────────────────────────────┘
                         │
         ┌───────────────▼──────────────┐
         │  Kafka UI (browse topics)    │
         │  http://localhost:9090       │
         └──────────────────────────────┘
```

---

## Kafka Topics

| Topic | Key | Value | Description |
|-------|-----|-------|-------------|
| `orders` | orderId | Order JSON | Raw order events from customers |
| `products` | productId | Product JSON | Product catalog (compacted) |
| `enriched-orders` | orderId | EnrichedOrder JSON | Orders + product details |
| `fraud-alerts` | orderId | OrderAlert JSON | High-value order alerts |
| `category-sales` | category | CategorySales JSON | Windowed aggregations |

---

## REST API Reference

### Orders

```bash
# Publish a custom order
POST http://localhost:8080/api/orders
Content-Type: application/json
{
  "customerId": "C001",
  "productId":  "P001",
  "category":   "Electronics",
  "amount":     2499.99,
  "quantity":   1
}

# Publish a batch
POST http://localhost:8080/api/orders/batch

# Simulate N random orders (burst)
POST http://localhost:8080/api/orders/simulate?count=20

# Start continuous simulation (1 order every 2s)
POST http://localhost:8080/api/orders/simulate/start?intervalMs=2000

# Stop simulation
POST http://localhost:8080/api/orders/simulate/stop

# Check simulation status
GET  http://localhost:8080/api/orders/simulate/status
```

### Analytics (Kafka Streams Interactive Queries)

```bash
# Category sales (last 2 minutes)
GET http://localhost:8080/api/analytics/category-sales

# Category sales (custom window)
GET http://localhost:8080/api/analytics/category-sales?windowMinutes=5

# All customer spending
GET http://localhost:8080/api/analytics/customer-spending

# Specific customer
GET http://localhost:8080/api/analytics/customer-spending/C001

# KafkaStreams state
GET http://localhost:8080/api/analytics/streams/status
```

---

## ksqlDB Usage

### Open CLI
```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

### Run init script (creates all streams and tables)
```sql
SET 'auto.offset.reset' = 'earliest';
RUN SCRIPT '/ksql/init.sql';
```

Or pipe from host:
```bash
docker exec -i ksqldb-server bash -c \
  "ksql http://localhost:8088 <<< \"$(cat ksql/init.sql)\""
```

### Key ksqlDB Queries

```sql
-- Live order feed
SELECT * FROM orders_stream EMIT CHANGES;

-- Category sales (real-time aggregation)
SELECT category, order_count, total_sales FROM category_sales_1min EMIT CHANGES;

-- Fraud alerts
SELECT * FROM fraud_alerts_stream EMIT CHANGES;

-- Customer spending
SELECT customer_id, total_spent FROM customer_spending_total EMIT CHANGES;
```

See `ksql/queries.sql` for the full query library.

---

## Project Structure

```
Kafka/
├── build.gradle.kts                        Gradle build (Kotlin DSL)
├── settings.gradle.kts
├── docker-compose.yml                      Full infrastructure
│
├── src/main/java/com/ecommerce/streaming/
│   ├── EcommerceStreamingApplication.java  Main entry point
│   ├── config/
│   │   ├── KafkaTopicConfig.java           Topic auto-creation
│   │   └── KafkaStreamsConfig.java         @EnableKafkaStreams + StreamsConfig
│   ├── model/
│   │   ├── Order.java                      Input event
│   │   ├── Product.java                    Product catalog entry
│   │   ├── EnrichedOrder.java              Joined result
│   │   ├── OrderAlert.java                 Fraud alert
│   │   └── CategorySales.java              Windowed aggregate
│   ├── serde/
│   │   └── JsonSerde.java                  Generic Jackson Serde<T>
│   ├── producer/
│   │   ├── OrderProducer.java              Kafka producer
│   │   └── ProductProducer.java            Catalog initialiser
│   ├── streams/
│   │   └── OrderStreamTopology.java        Full Kafka Streams topology
│   ├── service/
│   │   └── DataSimulator.java              Random order generator
│   └── controller/
│       ├── OrderController.java            /api/orders endpoints
│       └── AnalyticsController.java        /api/analytics endpoints
│
├── src/main/resources/
│   └── application.yml                     All configuration
│
├── ksql/
│   ├── init.sql                            Creates all ksqlDB streams/tables
│   └── queries.sql                         Demo query library
│
├── scripts/
│   ├── setup.sh                            First-time setup
│   └── demo.sh                             Automated demo walkthrough
│
└── docs/
    ├── README.md                           This file
    ├── answer-sheet.md                     Assignment answers
    ├── code-explanation.md                 Detailed code walkthrough
    └── commands.md                         All commands reference
```

---

## Stopping

```bash
# Stop Spring Boot: Ctrl+C in the terminal running bootRun

# Stop Docker services
docker compose down

# Stop and remove volumes (clean slate)
docker compose down -v
```

