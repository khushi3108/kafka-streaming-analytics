package com.ecommerce.streaming.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Creates all Kafka topics at application startup via Spring Kafka's AdminClient.
 *
 * Topics:
 *  - orders          : raw order events (key = orderId)
 *  - products        : product catalog, compacted (key = productId)
 *  - enriched-orders : orders joined with product info
 *  - fraud-alerts    : high-value order alerts
 *  - category-sales  : windowed category aggregations
 */
@Configuration
public class KafkaTopicConfig {

    // ─── Source Topics ────────────────────────────────────────────────

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name("orders")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic productsTopic() {
        return TopicBuilder.name("products")
                .partitions(3)
                .replicas(1)
                // Compacted: keeps only the latest value per key (product catalog)
                .compact()
                .build();
    }

    // ─── Sink Topics (created by Kafka Streams) ───────────────────────

    @Bean
    public NewTopic enrichedOrdersTopic() {
        return TopicBuilder.name("enriched-orders")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic fraudAlertsTopic() {
        return TopicBuilder.name("fraud-alerts")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic categorySalesTopic() {
        return TopicBuilder.name("category-sales")
                .partitions(3)
                .replicas(1)
                .build();
    }
}

