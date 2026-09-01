# Real-Time E-Commerce Order Analytics
### Kafka Streams + ksqlDB · Spring Boot 3 · Java 21 · Docker

> **Assignment:** Exploration of Streaming Architecture — Apache Kafka Streams & ksqlDB  
> **Use Case:** Real-time order processing, fraud detection, and category analytics for an e-commerce platform

---

## What This Project Demonstrates

| Layer | Technology | What It Shows |
|-------|-----------|--------------|
| **Stream Processing** | Kafka Streams Java API | KStream-KTable join, windowed aggregation, fraud filter, state stores |
| **Streaming SQL** | ksqlDB | Push/pull queries, tumbling & hopping windows, persistent materialized views |
| **Data Simulation** | Spring Boot REST | Order event production, continuous simulation |
| **Interactive Queries** | Kafka Streams IQ | Query live state stores directly from REST without a database |
| **Observability** | Kafka UI | Real-time topic browsing, consumer lag, message inspection |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  REST API  POST /api/orders/simulate                            │
│  Spring Boot :8090                                              │
└──────────────────────┬──────────────────────────────────────────┘
                       │ publishes JSON
              ┌────────▼────────┐    ┌──────────────┐
              │  orders topic   │    │ products topic│ ← catalog KTable
              └────────┬────────┘    └──────┬───────┘
                       │                    │
              ┌────────▼────────────────────▼────────┐
              │        Kafka Streams Topology          │
              │  1. KStream-KTable JOIN (enrich)       │──► enriched-orders
              │  2. filter(amount > $500)              │──► fraud-alerts
              │  3. groupBy(category).window(1 min)    │──► category-sales
              │  4. groupBy(customerId).aggregate()    │──► [state store]
              └────────────────────────────────────────┘
                       │ (same topics)
              ┌────────▼────────────────────────────────┐
              │         ksqlDB  :8088                    │
              │  STREAM  orders_stream                   │
              │  TABLE   category_sales_1min  (TUMBLING) │
              │  TABLE   category_sales_hourly (HOPPING) │
              │  TABLE   customer_spending               │
              │  STREAM  high_value_orders               │
              │  TABLE   fraud_by_category               │
              └──────────────────────────────────────────┘
                       │
              ┌────────▼──────────────┐
              │  Kafka UI  :9090      │  browse topics, messages, lag
              └───────────────────────┘
```

---

## Quick Start  *(3 commands)*

```bash
# 1. Start the entire Kafka infrastructure
docker compose up -d

# 2. Build and run the Spring Boot app  (wait ~60s for Docker services to be healthy first)
./gradlew bootRun

# 3. Trigger 30 random orders to start streaming
curl -X POST "http://localhost:8090/api/orders/simulate?count=30"
```

**That's it.** Topics are auto-created, ksqlDB streams/tables are auto-initialised.

---

## Service Ports

| Service | URL | Purpose |
|---------|-----|---------|
| Spring Boot API | http://localhost:8090 | Produce orders, query analytics |
| Kafka UI | http://localhost:9090 | Browse topics, messages, consumer lag |
| ksqlDB Server | http://localhost:8088 | Streaming SQL REST API |
| Schema Registry | http://localhost:8081 | Avro schema management |
| Kafka Brokers | localhost:9092, :9093, :9094 | 3-broker cluster (RF=3, min.insync.replicas=2) |

---

## Project Layout

```
Kafka/
├── docker-compose.yml          Full infra stack (auto topic creation + ksqlDB init)
├── build.gradle.kts            Gradle build (Kotlin DSL)
├── application.yml             Spring Boot config
│
├── docker/
│   ├── kafka-init.sh           Creates all 5 Kafka topics on first boot
│   └── ksql-init.sh            Pipes init.sql into ksqlDB on first boot
│
├── ksql/
│   ├── init.sql                ksqlDB stream/table definitions (auto-executed)
│   └── queries.sql             Demo query library for interactive exploration
│
├── src/main/java/.../
│   ├── config/                 KafkaStreamsConfig, KafkaTopicConfig
│   ├── model/                  Order, Product, EnrichedOrder, OrderAlert, CategorySales
│   ├── serde/                  Generic JsonSerde<T> for Kafka Streams
│   ├── streams/                OrderStreamTopology  ← main Kafka Streams logic
│   ├── producer/               OrderProducer, ProductProducer
│   ├── service/                DataSimulator
│   └── controller/             OrderController, AnalyticsController
│
└── docs/
    ├── commands.md             Step-by-step demo guide
    ├── answer-sheet.md         Assignment answers + deep exploration
    └── code-explanation.md     Code walkthrough
```

---

## Kafka Topics

| Topic | Partitions | Config | Producer |
|-------|-----------|--------|---------|
| `orders` | 3 | — | Spring Boot REST |
| `products` | 3 | compact | Spring Boot @PostConstruct |
| `enriched-orders` | 3 | — | Kafka Streams |
| `fraud-alerts` | 1 | — | Kafka Streams |
| `category-sales` | 3 | — | Kafka Streams |

---

## Stop Everything

```bash
# Stop Spring Boot
Ctrl+C

# Stop Docker infrastructure
docker compose down

# Full clean (removes all data)
docker compose down -v
```

