package com.ecommerce.streaming.producer;

import com.ecommerce.streaming.model.Product;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * Initialises the product catalog by publishing all products to the compacted
 * "products" Kafka topic. The Kafka Streams topology reads this topic as a
 * KTable (key = productId) for order-enrichment joins.
 *
 * <h2>Why this is NOT {@code @PostConstruct} any more</h2>
 *
 * <p>Seeding used to run from {@code @PostConstruct}, i.e. during bean initialisation, and
 * called {@code kafkaTemplate.send()} directly. {@code send()} is only asynchronous once the
 * producer HAS metadata: the first call blocks up to {@code max.block.ms} waiting for it and
 * then throws a {@code KafkaException} <em>synchronously</em> if no broker answers. Thrown from
 * {@code @PostConstruct} that becomes a {@code BeanCreationException}, so <b>the Spring context
 * could not start at all without a live broker</b> — every {@code @SpringBootTest} in the
 * project failed before it ran a single assertion, and a production pod that started a few
 * seconds ahead of Kafka crash-looped instead of waiting.
 *
 * <p>Two changes fix that while keeping production behaviour identical:
 * <ol>
 *   <li>Seeding moved to {@link ApplicationReadyEvent} — after the context is fully refreshed,
 *       so a failure degrades the catalog instead of killing the application.</li>
 *   <li>{@link #initializeCatalog()} never propagates an exception. A broker that is not up yet
 *       is logged as a warning; the compacted {@code products} topic can be re-seeded at any
 *       time by calling this method again, and the topology already left-joins so orders keep
 *       flowing (as "Unknown Product") in the meantime.</li>
 * </ol>
 *
 * <p>{@code app.catalog.seed-on-startup} (default {@code true}, so production is unchanged)
 * turns the startup seeding off entirely — which is what tests set.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProductProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    private static final String TOPIC = "products";

    /**
     * Whether to publish the catalog when the application becomes ready.
     * Defaults to {@code true} — production behaviour is unchanged. Tests set it to
     * {@code false} so no broker is needed to bring the context up.
     */
    @Value("${app.catalog.seed-on-startup:true}")
    private boolean seedOnStartup;

    /**
     * Seed the catalog once the application is fully started.
     *
     * <p>Deliberately an {@link ApplicationReadyEvent} listener rather than
     * {@code @PostConstruct}: bean initialisation must not depend on a remote system being
     * reachable. Never throws — see the class javadoc.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void seedCatalogOnStartup() {
        if (!seedOnStartup) {
            log.info("📦 Product catalog seeding disabled (app.catalog.seed-on-startup=false).");
            return;
        }
        initializeCatalog();
    }

    /**
     * Publish every catalog entry to the compacted {@code products} topic.
     *
     * <p>Resilient by contract: a serialization failure or an unreachable broker is logged and
     * skipped, never rethrown. Safe to call repeatedly — the topic is compacted and keyed by
     * productId, so re-seeding overwrites rather than duplicates.
     *
     * @return the number of products successfully handed to the producer
     */
    public int initializeCatalog() {
        log.info("📦 Initialising product catalog ({} products)...", CATALOG.size());
        int published = 0;
        for (Product product : CATALOG) {
            try {
                String json = objectMapper.writeValueAsString(product);
                // send() blocks for metadata on the first call and throws synchronously when the
                // broker is unreachable, so it has to be inside the try — not just the callback.
                kafkaTemplate.send(TOPIC, product.getProductId(), json)
                        .whenComplete((r, ex) -> {
                            if (ex == null) log.debug("  → Product [{}] published", product.getProductId());
                            else log.error("  → Failed to publish product [{}]", product.getProductId(), ex);
                        });
                published++;
            } catch (JsonProcessingException e) {
                log.error("Cannot serialize product {}", product.getProductId(), e);
            } catch (RuntimeException e) {
                // Typically KafkaException("Topic ... not present in metadata after N ms") when the
                // broker is not up yet. Log and continue: an unseeded catalog is a degraded
                // enrichment join, not a reason to take the application down.
                log.warn("  → Could not publish product [{}] ({}): {}",
                        product.getProductId(), e.getClass().getSimpleName(), e.getMessage());
            }
        }
        log.info("✅ Product catalog initialisation complete ({} of {} published).",
                published, CATALOG.size());
        return published;
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

