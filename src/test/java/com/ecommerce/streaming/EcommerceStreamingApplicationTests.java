package com.ecommerce.streaming;

import com.ecommerce.streaming.controller.AnalyticsController;
import com.ecommerce.streaming.producer.ProductProducer;
import com.ecommerce.streaming.service.InteractiveQueryService;
import com.ecommerce.streaming.streams.OrderStreamTopology;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Spring context must come up WITHOUT a Kafka broker.
 *
 * <p>Before the {@link ProductProducer} fix this test could not exist: catalog seeding ran from
 * {@code @PostConstruct} and threw synchronously when producer metadata timed out, so context
 * refresh failed and every {@code @SpringBootTest} in the project was impossible. This class is
 * the standing proof that it stays fixed.
 *
 * <p>Two properties keep it hermetic, and both are the same switches an operator would use:
 * <ul>
 *   <li>{@code app.catalog.seed-on-startup=false} — no catalog publish on
 *       {@code ApplicationReadyEvent} (defaults to true, so production is unchanged).</li>
 *   <li>{@code spring.kafka.streams.auto-startup=false} — the topology is still BUILT into the
 *       {@code StreamsBuilder} (so a broken topology fails this test), but {@code KafkaStreams}
 *       is not started and never dials a broker.</li>
 * </ul>
 */
@SpringBootTest(properties = {
        "app.catalog.seed-on-startup=false",
        "spring.kafka.streams.auto-startup=false",
        "spring.kafka.admin.auto-create=false",
        "spring.kafka.admin.fail-fast=false",
        "spring.kafka.bootstrap-servers=localhost:1",
        "spring.kafka.streams.bootstrap-servers=localhost:1"
})
class EcommerceStreamingApplicationTests {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private StreamsBuilder streamsBuilder;

    @Test
    @DisplayName("The application context starts with no broker reachable")
    void contextLoadsWithoutABroker() {
        assertThat(context.getBean(OrderStreamTopology.class)).isNotNull();
        assertThat(context.getBean(ProductProducer.class)).isNotNull();
        assertThat(context.getBean(InteractiveQueryService.class)).isNotNull();
        assertThat(context.getBean(AnalyticsController.class)).isNotNull();
    }

    @Test
    @DisplayName("The topology is registered and describes the expected state stores")
    void topologyIsBuiltAtStartup() {
        String description = streamsBuilder.build().describe().toString();

        assertThat(description)
                .contains(OrderStreamTopology.CATEGORY_SALES_STORE)
                .contains(OrderStreamTopology.CUSTOMER_SPENDING_STORE)
                .contains(OrderStreamTopology.PRODUCTS_STORE)
                .contains(OrderStreamTopology.CUSTOMER_VELOCITY_STORE)
                .contains(OrderStreamTopology.CUSTOMER_SESSION_STORE);
    }

    @Test
    @DisplayName("This instance advertises an application.server address for Interactive Queries")
    void applicationServerIsAdvertised() {
        // Left unset, Streams reports HostInfo("unavailable", -1) for every instance and the
        // multi-instance fan-out has no way to discover peers at all.
        HostInfo hostInfo = context.getBean(HostInfo.class);
        assertThat(hostInfo).isNotEqualTo(HostInfo.unavailable());
        assertThat(hostInfo.port()).isPositive();
    }
}
