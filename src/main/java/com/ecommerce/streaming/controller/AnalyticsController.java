package com.ecommerce.streaming.controller;

import com.ecommerce.streaming.model.CategorySales;
import com.ecommerce.streaming.streams.OrderStreamTopology;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.*;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.Duration;
import java.util.*;

/**
 * REST API for querying Kafka Streams Interactive Query state stores.
 *
 * Interactive Queries allow reading the local in-memory state stores
 * maintained by Kafka Streams – without going through another data store.
 *
 * Endpoints:
 *   GET /api/analytics/category-sales          – aggregated sales per category (last 1 min)
 *   GET /api/analytics/category-sales/all      – all windows ever seen
 *   GET /api/analytics/customer-spending       – running spend per customer
 *   GET /api/analytics/customer-spending/{id}  – spending for a specific customer
 *   GET /api/analytics/streams/status          – KafkaStreams state
 */
@Slf4j
@RestController
@RequestMapping("/api/analytics")
@RequiredArgsConstructor
public class AnalyticsController {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    // ── Category Sales (Windowed Store) ─────────────────────────────

    /**
     * Returns category sales aggregations for the last 2 minutes.
     * Queries the "category-sales-store" windowed state store via Interactive Queries.
     */
    @GetMapping("/category-sales")
    public ResponseEntity<Map<String, Object>> getCategorySales(
            @RequestParam(defaultValue = "2") long windowMinutes) {

        KafkaStreams streams = getRunningStreams();

        ReadOnlyWindowStore<String, CategorySales> store = streams.store(
                StoreQueryParameters.fromNameAndType(
                        OrderStreamTopology.CATEGORY_SALES_STORE,
                        QueryableStoreTypes.windowStore()
                )
        );

        Instant to = Instant.now();
        Instant from = to.minus(Duration.ofMinutes(windowMinutes));

        Map<String, CategorySales> results = new LinkedHashMap<>();
        try (KeyValueIterator<Windowed<String>, CategorySales> iter = store.fetchAll(from, to)) {
            while (iter.hasNext()) {
                KeyValue<Windowed<String>, CategorySales> entry = iter.next();
                // Merge windows for the same category (latest overwrites)
                results.merge(entry.key.key(), entry.value, (existing, incoming) -> {
                    // Pick the entry with more orders
                    return incoming.getOrderCount() > existing.getOrderCount() ? incoming : existing;
                });
            }
        }

        return ResponseEntity.ok(Map.of(
                "windowMinutes", windowMinutes,
                "fetchedAt", Instant.now().toString(),
                "categories", results
        ));
    }

    // ── Customer Spending (KeyValue Store) ──────────────────────────

    /**
     * Returns total spending accumulated per customer from the session-window store.
     */
    @GetMapping("/customer-spending")
    public ResponseEntity<Map<String, Object>> getCustomerSpending() {
        KafkaStreams streams = getRunningStreams();

        ReadOnlyKeyValueStore<String, Double> store = streams.store(
                StoreQueryParameters.fromNameAndType(
                        OrderStreamTopology.CUSTOMER_SPENDING_STORE,
                        QueryableStoreTypes.keyValueStore()
                )
        );

        Map<String, Double> spending = new TreeMap<>();
        try (KeyValueIterator<String, Double> iter = store.all()) {
            while (iter.hasNext()) {
                KeyValue<String, Double> entry = iter.next();
                spending.put(entry.key, Math.round(entry.value * 100.0) / 100.0);
            }
        }

        // Sort by spending descending
        List<Map<String, Object>> ranked = spending.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .map(e -> Map.<String, Object>of("customerId", e.getKey(), "totalSpent", e.getValue()))
                .toList();

        return ResponseEntity.ok(Map.of(
                "totalCustomers", ranked.size(),
                "fetchedAt", Instant.now().toString(),
                "customers", ranked
        ));
    }

    /**
     * Spending for a single customer.
     */
    @GetMapping("/customer-spending/{customerId}")
    public ResponseEntity<Map<String, Object>> getCustomerSpending(
            @org.springframework.web.bind.annotation.PathVariable String customerId) {

        KafkaStreams streams = getRunningStreams();

        ReadOnlyKeyValueStore<String, Double> store = streams.store(
                StoreQueryParameters.fromNameAndType(
                        OrderStreamTopology.CUSTOMER_SPENDING_STORE,
                        QueryableStoreTypes.keyValueStore()
                )
        );

        Double value = store.get(customerId);
        if (value == null) {
            return ResponseEntity.ok(Map.of("customerId", customerId, "totalSpent", 0.0, "found", false));
        }
        return ResponseEntity.ok(Map.of("customerId", customerId, "totalSpent", value, "found", true));
    }

    // ── Streams Status ───────────────────────────────────────────────

    @GetMapping("/streams/status")
    public ResponseEntity<Map<String, String>> streamsStatus() {
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        String state = (streams == null) ? "NOT_INITIALIZED" : streams.state().toString();
        return ResponseEntity.ok(Map.of("state", state));
    }

    // ── Helpers ──────────────────────────────────────────────────────

    private KafkaStreams getRunningStreams() {
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        if (streams == null) {
            throw new IllegalStateException("KafkaStreams is not initialized yet.");
        }
        if (streams.state() != KafkaStreams.State.RUNNING) {
            throw new IllegalStateException(
                    "KafkaStreams is not yet running. Current state: " + streams.state() +
                    ". Wait a moment and retry.");
        }
        return streams;
    }
}

