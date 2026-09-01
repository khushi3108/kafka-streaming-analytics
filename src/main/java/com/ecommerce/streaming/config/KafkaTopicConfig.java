package com.ecommerce.streaming.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
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
 *  - dead-letter     : records that failed deserialization, raw bytes + failure metadata
 *
 * <p><b>Durability.</b> Every topic is replication-factor 3 with min.insync.replicas=2, matching
 * the three-broker cluster in docker-compose.yml. RF=1 (the previous setting) is incompatible
 * with the exactly-once guarantee this app now claims: a transaction can only be as durable as
 * the partitions it writes to, so a single-replica topic means an acknowledged, committed
 * transaction disappears the moment its broker does. RF=3 + min.insync.replicas=2 tolerates the
 * loss of any one broker while still refusing writes if two are down (rather than silently
 * accepting data it cannot replicate).
 */
@Configuration
public class KafkaTopicConfig {

    /** Replication factor for every application topic — must be <= the broker count. */
    private static final int REPLICATION_FACTOR = 3;

    /**
     * Minimum in-sync replicas that must acknowledge a write for acks=all to succeed.
     * 2 of 3 survives a single broker loss; a third failure correctly fails writes instead of
     * accepting unreplicated data.
     */
    private static final String MIN_IN_SYNC_REPLICAS = "2";

    // ─── Source Topics ────────────────────────────────────────────────

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name("orders")
                .partitions(3)
                .replicas(REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_IN_SYNC_REPLICAS)
                .build();
    }

    @Bean
    public NewTopic productsTopic() {
        return TopicBuilder.name("products")
                .partitions(3)
                .replicas(REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_IN_SYNC_REPLICAS)
                // Compacted: keeps only the latest value per key (product catalog)
                .compact()
                .build();
    }

    // ─── Sink Topics (created by Kafka Streams) ───────────────────────

    @Bean
    public NewTopic enrichedOrdersTopic() {
        return TopicBuilder.name("enriched-orders")
                .partitions(3)
                .replicas(REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_IN_SYNC_REPLICAS)
                .build();
    }

    @Bean
    public NewTopic fraudAlertsTopic() {
        return TopicBuilder.name("fraud-alerts")
                .partitions(1)
                .replicas(REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_IN_SYNC_REPLICAS)
                .build();
    }

    @Bean
    public NewTopic categorySalesTopic() {
        return TopicBuilder.name("category-sales")
                .partitions(3)
                .replicas(REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_IN_SYNC_REPLICAS)
                .build();
    }

    // ─── Dead-Letter Topic ────────────────────────────────────────────

    /**
     * Destination for records that could not be deserialized, written by
     * {@link DeadLetterDeserializationExceptionHandler} with the raw bytes intact and the
     * failure details in record headers.
     *
     * <p>Retention is 30 days — longer than the brokers' 7-day default — because the whole
     * point of the DLQ is post-mortem analysis and replay after a fix, and a bad-data incident
     * is not always noticed inside a week.
     */
    @Bean
    public NewTopic deadLetterTopic() {
        return TopicBuilder.name(DeadLetterDeserializationExceptionHandler.DEFAULT_DEAD_LETTER_TOPIC)
                .partitions(3)
                .replicas(REPLICATION_FACTOR)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, MIN_IN_SYNC_REPLICAS)
                .config(TopicConfig.RETENTION_MS_CONFIG, Long.toString(30L * 24 * 60 * 60 * 1000))
                .build();
    }
}
