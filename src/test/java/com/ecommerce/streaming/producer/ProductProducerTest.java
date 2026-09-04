package com.ecommerce.streaming.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Catalog seeding must never be able to stop the application from starting.
 *
 * <p>This is the regression test for a real blocker. {@code initializeCatalog()} used to run from
 * {@code @PostConstruct} and call {@code kafkaTemplate.send()} directly. {@code send()} blocks for
 * producer metadata on its first call and throws a {@code KafkaException} SYNCHRONOUSLY when no
 * broker answers — which, from a {@code @PostConstruct}, becomes a {@code BeanCreationException}.
 * The Spring context could therefore not start without a live broker, so every
 * {@code @SpringBootTest} in the project failed before running a single assertion, and a pod that
 * started a few seconds ahead of Kafka crash-looped instead of waiting.
 */
class ProductProducerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @SuppressWarnings("unchecked")
    private KafkaTemplate<String, String> template() {
        return mock(KafkaTemplate.class);
    }

    private ProductProducer producer(KafkaTemplate<String, String> template, boolean seedOnStartup) {
        ProductProducer producer = new ProductProducer(template, objectMapper);
        ReflectionTestUtils.setField(producer, "seedOnStartup", seedOnStartup);
        return producer;
    }

    @Test
    @DisplayName("An unreachable broker does NOT propagate an exception out of catalog seeding")
    void brokerFailureDoesNotPropagate() {
        KafkaTemplate<String, String> template = template();
        // Exactly what a real send() does when the producer cannot fetch metadata in time.
        when(template.send(anyString(), anyString(), anyString()))
                .thenThrow(new KafkaException("Topic products not present in metadata after 60000 ms."));

        ProductProducer producer = producer(template, true);

        assertThatCode(producer::seedCatalogOnStartup)
                .as("a broker that is not up yet must degrade the catalog, not kill the application")
                .doesNotThrowAnyException();

        // It kept going rather than bailing on the first failure.
        verify(template, times(ProductProducer.CATALOG.size())).send(anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("initializeCatalog reports how many products it managed to publish")
    void successfulSeedingPublishesTheWholeCatalog() {
        KafkaTemplate<String, String> template = template();
        when(template.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        int published = producer(template, true).initializeCatalog();

        assertThat(published).isEqualTo(ProductProducer.CATALOG.size());
        verify(template, times(ProductProducer.CATALOG.size()))
                .send(eq("products"), anyString(), anyString());
    }

    @Test
    @DisplayName("app.catalog.seed-on-startup=false skips seeding entirely — no broker contact at all")
    void seedingCanBeDisabled() {
        KafkaTemplate<String, String> template = template();

        producer(template, false).seedCatalogOnStartup();

        verify(template, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("Seeding is keyed by productId so re-running it overwrites rather than duplicates")
    void catalogIsKeyedByProductId() {
        KafkaTemplate<String, String> template = template();
        when(template.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        producer(template, true).initializeCatalog();

        // The products topic is compacted; the key is what makes re-seeding idempotent.
        ProductProducer.CATALOG.forEach(product ->
                verify(template).send(eq("products"), eq(product.getProductId()), anyString()));
    }
}
