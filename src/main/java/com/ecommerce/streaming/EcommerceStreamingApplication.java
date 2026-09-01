package com.ecommerce.streaming;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * ╔══════════════════════════════════════════════════════════════════════╗
 * ║   Real-Time E-Commerce Order Analytics with Kafka Streams & ksqlDB  ║
 * ╠══════════════════════════════════════════════════════════════════════╣
 * ║                                                                      ║
 * ║  Architecture:                                                       ║
 * ║    [REST API] → [OrderProducer] → orders topic                       ║
 * ║                                                                      ║
 * ║  Kafka Streams Topology:                                             ║
 * ║    orders ──┬──► (join products KTable) ──► enriched-orders         ║
 * ║             ├──► (filter amount>500)    ──► fraud-alerts             ║
 * ║             ├──► (group by category,                                 ║
 * ║             │     tumbling 1-min window) ─► category-sales           ║
 * ║             └──► (group by customer)   ──► customer-spending-store   ║
 * ║                                                                      ║
 * ║  ksqlDB: SQL streaming queries over the same topics                  ║
 * ║                                                                      ║
 * ║  REST Endpoints:                                                     ║
 * ║    POST /api/orders                   Publish single order           ║
 * ║    POST /api/orders/simulate?count=N  Burst N random orders          ║
 * ║    POST /api/orders/simulate/start    Continuous simulation          ║
 * ║    POST /api/orders/simulate/stop     Stop simulation                ║
 * ║    GET  /api/analytics/category-sales  Category aggregations         ║
 * ║    GET  /api/analytics/customer-spending Customer totals             ║
 * ║    GET  /api/analytics/streams/status  KafkaStreams state            ║
 * ╚══════════════════════════════════════════════════════════════════════╝
 */
@Slf4j
@SpringBootApplication
@EnableScheduling
public class EcommerceStreamingApplication {

    public static void main(String[] args) {
        SpringApplication.run(EcommerceStreamingApplication.class, args);
        log.info("\n\n" +
                "  ┌─────────────────────────────────────────────────────────┐\n" +
                "  │  🛒  E-Commerce Kafka Streaming App started              │\n" +
                "  │     API:       http://localhost:8080                     │\n" +
                "  │     Actuator:  http://localhost:8080/actuator/health     │\n" +
                "  │     Kafka UI:  http://localhost:9090                     │\n" +
                "  │     ksqlDB:    http://localhost:8088                     │\n" +
                "  └─────────────────────────────────────────────────────────┘\n");
    }
}

