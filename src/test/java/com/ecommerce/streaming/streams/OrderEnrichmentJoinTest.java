package com.ecommerce.streaming.streams;

import com.ecommerce.streaming.model.EnrichedOrder;
import com.ecommerce.streaming.model.Product;
import com.ecommerce.streaming.support.TopologyTestFixture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static com.ecommerce.streaming.support.TopologyTestFixture.at;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The KStream–KTable enrichment join.
 *
 * <p>The load-bearing property is that it is a LEFT join: an order for a product that is not in
 * the catalog yet must still reach {@code enriched-orders} (degraded to "Unknown Product"),
 * because the catalog is seeded asynchronously and orders can legitimately arrive first. If the
 * join were an inner join, those orders would vanish — silently, with no error anywhere.
 */
class OrderEnrichmentJoinTest {

    private TopologyTestFixture fixture;

    @BeforeEach
    void setUp() {
        fixture = new TopologyTestFixture();
    }

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    @Test
    @DisplayName("A known product populates productName and brand on the enriched order")
    void knownProductEnrichesTheOrder() {
        fixture.sendProduct(new Product("P001", "MacBook Pro 16\"", "Electronics",
                new BigDecimal("2499.99"), "Apple", "M3 Pro chip, 18GB RAM"));

        fixture.sendOrder(com.ecommerce.streaming.model.Order.builder()
                .orderId("O-1").customerId("C-1").productId("P001")
                .category("Electronics").amount(new BigDecimal("2499.99"))
                .quantity(1).status("CONFIRMED").timestamp(at(10).toEpochMilli())
                .build());

        List<EnrichedOrder> enriched = fixture.readEnrichedOrders();
        assertThat(enriched).hasSize(1);

        EnrichedOrder e = enriched.get(0);
        assertThat(e.getOrderId()).isEqualTo("O-1");
        assertThat(e.getProductName()).isEqualTo("MacBook Pro 16\"");
        assertThat(e.getBrand()).isEqualTo("Apple");
        assertThat(e.getCategory()).isEqualTo("Electronics");
        assertThat(e.getAmount()).isEqualByComparingTo(new BigDecimal("2499.99"));
        // The enriched record is re-keyed back to orderId for downstream consumers.
        assertThat(e.getTimestamp()).isEqualTo(at(10).toEpochMilli());
    }

    @Test
    @DisplayName("A missing product DEGRADES to 'Unknown Product' — the order is never dropped")
    void missingProductDegradesInsteadOfDroppingTheOrder() {
        // No product published at all: the KTable is empty.
        fixture.sendOrder(com.ecommerce.streaming.model.Order.builder()
                .orderId("O-404").customerId("C-1").productId("P-DOES-NOT-EXIST")
                .category("Books").amount(new BigDecimal("19.99"))
                .quantity(2).status("CONFIRMED").timestamp(at(10).toEpochMilli())
                .build());

        List<EnrichedOrder> enriched = fixture.readEnrichedOrders();
        assertThat(enriched)
                .as("a LEFT join must pass the order through, not swallow it")
                .hasSize(1);

        EnrichedOrder e = enriched.get(0);
        assertThat(e.getOrderId()).isEqualTo("O-404");
        assertThat(e.getProductName()).isEqualTo("Unknown Product");
        assertThat(e.getBrand()).isEqualTo("Unknown Brand");
        // Everything the ORDER itself carried survives; only the catalog columns degrade.
        assertThat(e.getCategory()).isEqualTo("Books");
        assertThat(e.getAmount()).isEqualByComparingTo(new BigDecimal("19.99"));
        assertThat(e.getQuantity()).isEqualTo(2);
    }

    @Test
    @DisplayName("A product published after the order enriches subsequent orders only")
    void catalogUpdatesApplyToLaterOrders() {
        fixture.sendOrder(com.ecommerce.streaming.model.Order.builder()
                .orderId("O-early").customerId("C-1").productId("P009")
                .category("Clothing").amount(new BigDecimal("69.99"))
                .quantity(1).status("CONFIRMED").timestamp(at(10).toEpochMilli())
                .build());

        fixture.sendProduct(new Product("P009", "Levi's 501 Jeans", "Clothing",
                new BigDecimal("69.99"), "Levi's", "Classic straight-leg denim"));

        fixture.sendOrder(com.ecommerce.streaming.model.Order.builder()
                .orderId("O-late").customerId("C-2").productId("P009")
                .category("Clothing").amount(new BigDecimal("69.99"))
                .quantity(1).status("CONFIRMED").timestamp(at(20).toEpochMilli())
                .build());

        List<EnrichedOrder> enriched = fixture.readEnrichedOrders();
        assertThat(enriched).hasSize(2);
        assertThat(enriched.get(0).getProductName()).isEqualTo("Unknown Product");
        assertThat(enriched.get(1).getProductName()).isEqualTo("Levi's 501 Jeans");
        assertThat(enriched.get(1).getBrand()).isEqualTo("Levi's");
    }
}
