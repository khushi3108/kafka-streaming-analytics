package com.ecommerce.streaming.producer;

import com.ecommerce.streaming.model.Product;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * Initialises the product catalog by publishing all products to the compacted
 * "products" Kafka topic. The Kafka Streams topology reads this topic as a
 * KTable (key = productId) for order-enrichment joins.
 *
 * @PostConstruct ensures catalog is populated before the first order arrives.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProductProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    private static final String TOPIC = "products";

    @PostConstruct
    public void initializeCatalog() {
        log.info("📦 Initialising product catalog ({} products)...", CATALOG.size());
        CATALOG.forEach(product -> {
            try {
                String json = objectMapper.writeValueAsString(product);
                kafkaTemplate.send(TOPIC, product.getProductId(), json)
                        .whenComplete((r, ex) -> {
                            if (ex == null) log.debug("  → Product [{}] published", product.getProductId());
                            else log.error("  → Failed to publish product [{}]", product.getProductId(), ex);
                        });
            } catch (JsonProcessingException e) {
                log.error("Cannot serialize product {}", product.getProductId(), e);
            }
        });
        log.info("✅ Product catalog initialisation complete.");
    }

    // ── Static product catalog ─────────────────────────────────────────
    public static final List<Product> CATALOG = List.of(

            // Electronics
            new Product("P001", "MacBook Pro 16\"",      "Electronics", new BigDecimal("2499.99"), "Apple",   "M3 Pro chip, 18GB RAM"),
            new Product("P002", "Dell XPS 15",           "Electronics", new BigDecimal("1799.99"), "Dell",    "OLED 4K display, Intel i9"),
            new Product("P003", "Sony WH-1000XM5",       "Electronics",  new BigDecimal("349.99"), "Sony",    "ANC Wireless Headphones"),
            new Product("P004", "Samsung Galaxy S24",    "Electronics",  new BigDecimal("999.99"), "Samsung", "200MP camera, AI features"),
            new Product("P005", "iPad Pro 12.9\"",       "Electronics", new BigDecimal("1099.99"), "Apple",   "M4 chip, OLED display"),
            new Product("P006", "NVIDIA RTX 4080",       "Electronics",  new BigDecimal("799.99"), "NVIDIA",  "GPU – 16GB GDDR6X"),
            new Product("P007", "LG OLED 65\" TV",      "Electronics", new BigDecimal("1599.99"), "LG",      "4K 120Hz Smart TV"),

            // Clothing
            new Product("P008", "Nike Air Max 2024",    "Clothing",     new BigDecimal("189.99"), "Nike",    "Running shoes, mesh upper"),
            new Product("P009", "Levi's 501 Jeans",     "Clothing",      new BigDecimal("69.99"), "Levi's",  "Classic straight-leg denim"),
            new Product("P010", "North Face Jacket",    "Clothing",     new BigDecimal("249.99"), "NF",      "Waterproof down jacket"),
            new Product("P011", "Adidas Ultraboost 22", "Clothing",     new BigDecimal("159.99"), "Adidas",  "Responsive running shoe"),

            // Books
            new Product("P012", "Clean Code",           "Books",         new BigDecimal("35.99"), "O'Reilly","Robert C. Martin"),
            new Product("P013", "System Design Interview","Books",        new BigDecimal("39.99"), "ByteByteGo","Alex Xu"),
            new Product("P014", "Designing Data-Intensive Applications","Books", new BigDecimal("49.99"), "O'Reilly", "Martin Kleppmann"),

            // Food & Grocery
            new Product("P015", "Organic Coffee Beans", "Food",          new BigDecimal("24.99"), "Starbucks","1kg whole bean, dark roast"),
            new Product("P016", "Protein Powder 2kg",  "Food",           new BigDecimal("54.99"), "Optimum", "Whey chocolate flavour"),
            new Product("P017", "Manuka Honey 500g",   "Food",           new BigDecimal("32.99"), "Comvita", "UMF 15+ certified"),

            // Sports
            new Product("P018", "Yoga Mat Pro",         "Sports",        new BigDecimal("59.99"), "Manduka", "6mm thick, non-slip"),
            new Product("P019", "Kettlebell 20kg",      "Sports",        new BigDecimal("79.99"), "Rogue",   "Cast iron, powder coated"),
            new Product("P020", "Garmin Forerunner 955","Sports",        new BigDecimal("499.99"), "Garmin", "GPS multisport watch")
    );
}

