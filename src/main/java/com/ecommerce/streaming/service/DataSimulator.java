package com.ecommerce.streaming.service;

import com.ecommerce.streaming.model.Order;
import com.ecommerce.streaming.producer.OrderProducer;
import com.ecommerce.streaming.producer.ProductProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Simulates real-time e-commerce order traffic by generating random orders
 * and publishing them to the "orders" Kafka topic.
 *
 * <p>Designed to demonstrate:
 *  - High-frequency streaming (adjustable rate)
 *  - Mix of normal and high-value orders (to trigger fraud alerts)
 *  - Spread across all product categories (to exercise category aggregations)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DataSimulator {

    private final OrderProducer orderProducer;

    private static final Random RANDOM = new Random();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread simulationThread;

    // ── Reference data ──────────────────────────────────────────────
    private static final List<String> CUSTOMER_IDS = List.of(
            "C001", "C002", "C003", "C004", "C005",
            "C006", "C007", "C008", "C009", "C010",
            "C011", "C012", "C013", "C014", "C015"
    );

    private static final List<String> CATEGORIES = List.of(
            "Electronics", "Clothing", "Books", "Food", "Sports"
    );

    // ── Public API ──────────────────────────────────────────────────

    /**
     * Generate {@code count} random orders immediately (burst mode).
     */
    public int generateOrders(int count) {
        log.info("🏭 Generating {} random orders...", count);
        for (int i = 0; i < count; i++) {
            orderProducer.sendOrder(buildRandomOrder());
        }
        return count;
    }

    /**
     * Start a background thread that emits one order every {@code intervalMs} milliseconds.
     * Calling this while already running resets the interval.
     */
    public void startContinuousSimulation(long intervalMs) {
        if (running.getAndSet(true)) {
            log.info("⏩ Simulation already running – restarting with new interval {}ms", intervalMs);
            stopSimulation();
            running.set(true);
        }
        simulationThread = new Thread(() -> {
            log.info("▶ Continuous simulation started (interval={}ms)", intervalMs);
            while (running.get()) {
                try {
                    orderProducer.sendOrder(buildRandomOrder());
                    Thread.sleep(intervalMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            log.info("⏹ Continuous simulation stopped");
        }, "order-simulator");
        simulationThread.setDaemon(true);
        simulationThread.start();
    }

    public void stopSimulation() {
        running.set(false);
        if (simulationThread != null) {
            simulationThread.interrupt();
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    // ── Order builder ───────────────────────────────────────────────

    private Order buildRandomOrder() {
        // Pick a random product from catalog
        var product = ProductProducer.CATALOG.get(RANDOM.nextInt(ProductProducer.CATALOG.size()));
        String customerId = CUSTOMER_IDS.get(RANDOM.nextInt(CUSTOMER_IDS.size()));

        // Occasionally inject a high-value order (~15% chance) to trigger fraud alerts
        BigDecimal amount;
        int quantity;
        if (RANDOM.nextDouble() < 0.15) {
            // High-value order: multiply by 2-5
            quantity = RANDOM.nextInt(3) + 2;
            BigDecimal surgeMultiplier = BigDecimal.valueOf(1.0 + RANDOM.nextDouble());
            amount = product.getPrice()
                    .multiply(BigDecimal.valueOf(quantity))
                    .multiply(surgeMultiplier);
        } else {
            quantity = RANDOM.nextInt(3) + 1;
            amount = product.getPrice().multiply(BigDecimal.valueOf(quantity));
        }

        // Normalise to cents – BigDecimal, so the value published is exactly what
        // downstream aggregation will sum (no binary-float surprises).
        amount = amount.setScale(2, RoundingMode.HALF_UP);

        return Order.builder()
                .orderId(UUID.randomUUID().toString())
                .customerId(customerId)
                .productId(product.getProductId())
                .category(product.getCategory())
                .amount(amount)
                .quantity(quantity)
                .status("PENDING")
                .timestamp(System.currentTimeMillis())
                .build();
    }
}

