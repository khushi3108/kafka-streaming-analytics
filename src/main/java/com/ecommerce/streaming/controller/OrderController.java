package com.ecommerce.streaming.controller;

import com.ecommerce.streaming.model.Order;
import com.ecommerce.streaming.producer.OrderProducer;
import com.ecommerce.streaming.service.DataSimulator;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * REST API for producing orders to Kafka.
 *
 * Endpoints:
 *   POST /api/orders                   – publish a single custom order
 *   POST /api/orders/batch             – publish a list of orders
 *   POST /api/orders/simulate?count=N  – generate N random orders immediately
 *   POST /api/orders/simulate/start    – start continuous simulation
 *   POST /api/orders/simulate/stop     – stop continuous simulation
 *   GET  /api/orders/simulate/status   – check if simulation is running
 */
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducer orderProducer;
    private final DataSimulator simulator;

    // ── Single order ────────────────────────────────────────────────

    /**
     * Publish a custom order.
     * Auto-fills orderId and timestamp if not provided.
     */
    @PostMapping
    public ResponseEntity<Order> createOrder(@RequestBody Order order) {
        if (order.getOrderId() == null || order.getOrderId().isBlank()) {
            order.setOrderId(UUID.randomUUID().toString());
        }
        if (order.getTimestamp() == 0) {
            order.setTimestamp(Instant.now().toEpochMilli());
        }
        orderProducer.sendOrder(order);
        return ResponseEntity.accepted().body(order);
    }

    // ── Batch orders ────────────────────────────────────────────────

    @PostMapping("/batch")
    public ResponseEntity<Map<String, Object>> createBatch(@RequestBody List<Order> orders) {
        orders.forEach(order -> {
            if (order.getOrderId() == null) order.setOrderId(UUID.randomUUID().toString());
            if (order.getTimestamp() == 0) order.setTimestamp(Instant.now().toEpochMilli());
            orderProducer.sendOrder(order);
        });
        return ResponseEntity.accepted().body(Map.of(
                "status", "accepted",
                "count", orders.size()
        ));
    }

    // ── Simulation ──────────────────────────────────────────────────

    @PostMapping("/simulate")
    public ResponseEntity<Map<String, Object>> simulateBurst(
            @RequestParam(defaultValue = "20") int count) {
        int sent = simulator.generateOrders(count);
        return ResponseEntity.ok(Map.of(
                "status", "done",
                "ordersGenerated", sent
        ));
    }

    @PostMapping("/simulate/start")
    public ResponseEntity<Map<String, Object>> startSimulation(
            @RequestParam(defaultValue = "2000") long intervalMs) {
        simulator.startContinuousSimulation(intervalMs);
        return ResponseEntity.ok(Map.of(
                "status", "started",
                "intervalMs", intervalMs,
                "message", "Sending 1 order every " + intervalMs + "ms. POST /simulate/stop to halt."
        ));
    }

    @PostMapping("/simulate/stop")
    public ResponseEntity<Map<String, String>> stopSimulation() {
        simulator.stopSimulation();
        return ResponseEntity.ok(Map.of("status", "stopped"));
    }

    @GetMapping("/simulate/status")
    public ResponseEntity<Map<String, Object>> simulationStatus() {
        return ResponseEntity.ok(Map.of(
                "running", simulator.isRunning()
        ));
    }
}

