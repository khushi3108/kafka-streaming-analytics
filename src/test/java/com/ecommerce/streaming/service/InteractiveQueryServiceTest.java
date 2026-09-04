package com.ecommerce.streaming.service;

import com.ecommerce.streaming.model.CustomerSpending;
import com.ecommerce.streaming.model.query.CategorySalesResponse;
import com.ecommerce.streaming.model.query.CategorySalesWindow;
import com.ecommerce.streaming.model.query.CustomerSpendingEntry;
import com.ecommerce.streaming.model.query.CustomerSpendingResponse;
import com.ecommerce.streaming.model.query.CustomerSpendingResult;
import com.ecommerce.streaming.model.query.QueryFailure;
import com.ecommerce.streaming.streams.OrderStreamTopology;
import com.ecommerce.streaming.support.ListKeyValueIterator;
import com.ecommerce.streaming.support.StubHttpTransport;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The multi-instance Interactive Query fan-out — merge semantics, standby de-duplication,
 * partial-result reporting and the recursion guard.
 *
 * <p>No broker and no network: {@link KafkaStreams} is mocked for discovery and the local store
 * read, and the peer RPC leg runs through a REAL {@link RestClient} whose transport is
 * {@link StubHttpTransport}. Using a real RestClient matters — the URI it builds (including the
 * {@code local=true} guard) and the JSON it parses are both under test, and a mocked
 * {@code RestClient} would assert neither.
 *
 * <p>Every claim here is about SILENT wrongness. The endpoint this class covers once returned
 * half the customers with HTTP 200 and no indication anything was missing; each test below pins
 * one of the ways that could come back.
 */
class InteractiveQueryServiceTest {

    private static final String SPENDING_STORE = OrderStreamTopology.CUSTOMER_SPENDING_STORE;
    private static final String SALES_STORE    = OrderStreamTopology.CATEGORY_SALES_STORE;

    private static final HostInfo LOCAL  = new HostInfo("localhost", 8090);
    private static final HostInfo PEER_A = new HostInfo("10.0.0.2", 8090);
    private static final HostInfo PEER_B = new HostInfo("10.0.0.3", 8090);

    /** Same two settings the production JsonSerde uses, so the stubbed wire format is realistic. */
    private static final ObjectMapper JSON = new ObjectMapper()
            .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);

    private StreamsBuilderFactoryBean factoryBean;
    private KafkaStreams streams;
    private StubHttpTransport transport;
    private InteractiveQueryService service;

    @BeforeEach
    void setUp() {
        factoryBean = mock(StreamsBuilderFactoryBean.class);
        streams = mock(KafkaStreams.class);
        when(factoryBean.getKafkaStreams()).thenReturn(streams);
        when(streams.state()).thenReturn(KafkaStreams.State.RUNNING);

        transport = new StubHttpTransport();
        RestClient restClient = RestClient.builder().requestFactory(transport).build();

        service = new InteractiveQueryService(factoryBean, restClient, LOCAL, "http", 5_000L);
    }

    // ═══════════════════════════════════════════════════════════════════
    //  1. The fan-out MERGES; it does not pick a winner
    // ═══════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("Two hosts returning different windows: BOTH appear, and a shared window is SUMMED")
    void unkeyedFanOutMergesShardsInsteadOfPickingOne() throws Exception {
        givenStoreHostedBy(SALES_STORE, active(PEER_A), active(PEER_B));

        transport.respondJson(StubHttpTransport.hostPort(PEER_A), JSON.writeValueAsString(
                CategorySalesResponse.builder()
                        .localOnly(true)
                        .windows(List.of(
                                salesWindow("Electronics", 1_000, 61_000, 2, "100.00"),
                                salesWindow("Books", 1_000, 61_000, 1, "10.00")))
                        .build()));

        transport.respondJson(StubHttpTransport.hostPort(PEER_B), JSON.writeValueAsString(
                CategorySalesResponse.builder()
                        .localOnly(true)
                        .windows(List.of(
                                // SAME (category, window) as peer A — the collision case.
                                salesWindow("Electronics", 1_000, 61_000, 3, "50.00"),
                                // A window peer A knows nothing about.
                                salesWindow("Electronics", 61_000, 121_000, 4, "400.00")))
                        .build()));

        CategorySalesResponse result = service.categorySales(
                Instant.ofEpochMilli(0), Instant.ofEpochMilli(200_000), false);

        assertThat(result.isPartial()).isFalse();
        assertThat(result.getHostsQueried())
                .containsExactlyInAnyOrder("10.0.0.2:8090", "10.0.0.3:8090");

        // Three distinct (category, window) buckets survive. Keying the merge on category alone —
        // the bug this replaced — would have collapsed the two Electronics windows into one.
        assertThat(result.getWindowCount()).isEqualTo(3);
        assertThat(result.getWindows())
                .extracting(CategorySalesWindow::getCategory, CategorySalesWindow::getWindowStartMs)
                .containsExactly(
                        org.assertj.core.api.Assertions.tuple("Books", 1_000L),
                        org.assertj.core.api.Assertions.tuple("Electronics", 1_000L),
                        org.assertj.core.api.Assertions.tuple("Electronics", 61_000L));

        CategorySalesWindow shared = result.getWindows().stream()
                .filter(w -> "Electronics".equals(w.getCategory()) && w.getWindowStartMs() == 1_000L)
                .findFirst().orElseThrow();
        assertThat(shared.getOrderCount())
                .as("the same (category, window) from two shards must SUM, never pick a winner")
                .isEqualTo(5);
        assertThat(shared.getTotalSales()).isEqualByComparingTo(new BigDecimal("150.00"));
        // The average is recomputed from the summed totals, not averaged from two averages.
        assertThat(shared.getAvgOrderValue()).isEqualByComparingTo(new BigDecimal("30.00"));

        // Every outbound call carries the recursion guard.
        assertThat(transport.requestedUris())
                .hasSize(2)
                .allSatisfy(uri -> assertThat(uri.getQuery()).contains("local=true"));
    }

    @Test
    @DisplayName("The local shard and a remote shard are merged, and a customer on both is SUMMED")
    void localAndRemoteShardsAreMerged() throws Exception {
        givenStoreHostedBy(SPENDING_STORE, active(LOCAL), active(PEER_A));
        givenLocalSpendingStore(
                spending("C1", "100.00", 1, 1_000L),
                spending("C2", "200.00", 2, 2_000L));

        transport.respondJson(StubHttpTransport.hostPort(PEER_A), JSON.writeValueAsString(
                CustomerSpendingResponse.builder()
                        .localOnly(true)
                        .customers(List.of(
                                entry("C1", "50.00", 1, 9_000L),   // also seen locally
                                entry("C3", "300.00", 3, 3_000L))) // only on the peer
                        .build()));

        CustomerSpendingResponse result = service.allCustomerSpending(false);

        assertThat(result.isPartial()).isFalse();
        assertThat(result.getTotalCustomers()).isEqualTo(3);
        // Ranked by totalSpent descending.
        assertThat(result.getCustomers()).extracting(CustomerSpendingEntry::getCustomerId)
                .containsExactly("C3", "C2", "C1");

        CustomerSpendingEntry c1 = result.getCustomers().get(2);
        assertThat(c1.getTotalSpent())
                .as("a key seen on two shards is summed, not overwritten")
                .isEqualByComparingTo(new BigDecimal("150.00"));
        assertThat(c1.getOrderCount()).isEqualTo(2);
        assertThat(c1.getAvgOrderValue()).isEqualByComparingTo(new BigDecimal("75.00"));
        assertThat(c1.getLastOrderTimestamp())
                .as("lastOrderTimestamp folds as a max across shards")
                .isEqualTo(9_000L);
    }

    // ═══════════════════════════════════════════════════════════════════
    //  2. Standby de-duplication
    // ═══════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("A standby-only host is SKIPPED — querying it would DOUBLE every figure")
    void standbyOnlyHostIsSkipped() throws Exception {
        // PEER_B holds the store only as a STANDBY: a full copy of PEER_A's partitions, not a
        // different shard of them. The merge SUMS (shards are disjoint by key), so including a
        // standby would silently double everything it duplicated.
        givenStoreHostedBy(SPENDING_STORE, active(PEER_A), standbyOnly(PEER_B));

        String shard = JSON.writeValueAsString(CustomerSpendingResponse.builder()
                .localOnly(true)
                .customers(List.of(entry("C1", "100.00", 2, 1_000L)))
                .build());
        transport.respondJson(StubHttpTransport.hostPort(PEER_A), shard);
        // Deliberately routed too: if the standby WERE queried it would answer successfully and
        // the totals would silently double instead of failing loudly.
        transport.respondJson(StubHttpTransport.hostPort(PEER_B), shard);

        CustomerSpendingResponse result = service.allCustomerSpending(false);

        assertThat(transport.requestedHosts())
                .as("the standby host must never be contacted")
                .containsExactly("10.0.0.2:8090");
        assertThat(result.getHostsQueried()).containsExactly("10.0.0.2:8090");
        assertThat(result.getTotalCustomers()).isEqualTo(1);
        assertThat(result.getCustomers().get(0).getTotalSpent())
                .as("100.00, not the 200.00 a double-counted standby would produce")
                .isEqualByComparingTo(new BigDecimal("100.00"));
        assertThat(result.getCustomers().get(0).getOrderCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("A host holding the store as BOTH active and standby is still queried once")
    void hostThatIsActiveAndStandbyIsStillQueried() throws Exception {
        // The skip is deliberately conservative: only an instance that lists the store as a
        // standby AND NOT as an active is dropped. Dropping an active host would be the original
        // silent-data-loss bug wearing a different hat.
        StreamsMetadata both = mock(StreamsMetadata.class);
        when(both.hostInfo()).thenReturn(PEER_A);
        when(both.stateStoreNames()).thenReturn(Set.of(SPENDING_STORE));
        when(both.standbyStateStoreNames()).thenReturn(Set.of(SPENDING_STORE));
        when(streams.streamsMetadataForStore(SPENDING_STORE)).thenReturn(List.of(both));

        transport.respondJson(StubHttpTransport.hostPort(PEER_A), JSON.writeValueAsString(
                CustomerSpendingResponse.builder()
                        .localOnly(true)
                        .customers(List.of(entry("C1", "100.00", 2, 1_000L)))
                        .build()));

        CustomerSpendingResponse result = service.allCustomerSpending(false);

        assertThat(transport.requestedHosts()).containsExactly("10.0.0.2:8090");
        assertThat(result.getTotalCustomers()).isEqualTo(1);
    }

    // ═══════════════════════════════════════════════════════════════════
    //  3. Partial results say they are partial
    // ═══════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("A failing peer sets partial=true and is NAMED in failures[] with a reason")
    void peerFailureProducesPartialResultNamingTheHost() throws Exception {
        givenStoreHostedBy(SPENDING_STORE, active(PEER_A), active(PEER_B));

        transport.respondJson(StubHttpTransport.hostPort(PEER_A), JSON.writeValueAsString(
                CustomerSpendingResponse.builder()
                        .localOnly(true)
                        .customers(List.of(entry("C1", "100.00", 1, 1_000L)))
                        .build()));
        transport.respondStatus(StubHttpTransport.hostPort(PEER_B), HttpStatus.INTERNAL_SERVER_ERROR);

        CustomerSpendingResponse result = service.allCustomerSpending(false);

        assertThat(result.isPartial())
                .as("a subset of the answer must never be indistinguishable from the whole answer")
                .isTrue();
        assertThat(result.getFailures()).hasSize(1);
        QueryFailure failure = result.getFailures().get(0);
        assertThat(failure.getHost()).isEqualTo("10.0.0.3:8090");
        assertThat(failure.getReason()).isEqualTo("RPC_FAILED");
        assertThat(failure.getDetail()).isNotBlank();

        // The shard that DID answer is still returned — degraded, not discarded.
        assertThat(result.getCustomers()).extracting(CustomerSpendingEntry::getCustomerId)
                .containsExactly("C1");
        assertThat(result.getHostsQueried())
                .containsExactlyInAnyOrder("10.0.0.2:8090", "10.0.0.3:8090");
    }

    @Test
    @DisplayName("An instance with no advertised application.server is reported, not ignored")
    void hostWithoutApplicationServerIsReportedAsAFailure() throws Exception {
        givenStoreHostedBy(SPENDING_STORE, active(PEER_A), active(HostInfo.unavailable()));

        transport.respondJson(StubHttpTransport.hostPort(PEER_A), JSON.writeValueAsString(
                CustomerSpendingResponse.builder()
                        .localOnly(true)
                        .customers(List.of(entry("C1", "100.00", 1, 1_000L)))
                        .build()));

        CustomerSpendingResponse result = service.allCustomerSpending(false);

        assertThat(result.isPartial()).isTrue();
        assertThat(result.getFailures())
                .extracting(QueryFailure::getReason)
                .containsExactly("NO_APPLICATION_SERVER");
    }

    @Test
    @DisplayName("When EVERY host fails the request 503s rather than returning an empty list")
    void totalFailureIsAnErrorNotAnEmptyAnswer() {
        givenStoreHostedBy(SPENDING_STORE, active(PEER_A), active(PEER_B));
        transport.respondStatus(StubHttpTransport.hostPort(PEER_A), HttpStatus.INTERNAL_SERVER_ERROR);
        transport.respondStatus(StubHttpTransport.hostPort(PEER_B), HttpStatus.INTERNAL_SERVER_ERROR);

        // An empty list with partial=true is technically honest but reads as "there is no data".
        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> service.allCustomerSpending(false))
                .isInstanceOf(com.ecommerce.streaming.exception.StreamsNotReadyException.class);
    }

    // ═══════════════════════════════════════════════════════════════════
    //  4. The local=true recursion guard
    // ═══════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("An unkeyed local=true scan issues NO outbound RPC and skips discovery entirely")
    void localOnlyScanNeverCallsAPeer() {
        givenLocalSpendingStore(spending("C1", "100.00", 1, 1_000L));

        CustomerSpendingResponse result = service.allCustomerSpending(true);

        assertThat(transport.requestedUris())
                .as("without this guard every instance fans out to every instance, forever")
                .isEmpty();
        verify(streams, never()).streamsMetadataForStore(anyString());

        assertThat(result.isLocalOnly()).isTrue();
        assertThat(result.isPartial()).isFalse();
        assertThat(result.getHostsQueried()).containsExactly("localhost:8090");
        assertThat(result.getTotalCustomers()).isEqualTo(1);
    }

    @Test
    @DisplayName("A keyed local=true lookup issues NO outbound RPC and does not consult key metadata")
    void localOnlyKeyedLookupNeverCallsAPeer() {
        givenLocalSpendingStoreValue("C1", spendingValue("C1", "100.00", 1, 1_000L));

        CustomerSpendingResult result = service.customerSpending("C1", true);

        assertThat(transport.requestedUris()).isEmpty();
        verify(streams, never()).queryMetadataForKey(anyString(), any(), any(org.apache.kafka.common.serialization.Serializer.class));

        assertThat(result.isFound()).isTrue();
        assertThat(result.isServedLocally()).isTrue();
        assertThat(result.getServedBy()).isEqualTo("localhost:8090");
        assertThat(result.getTotalSpent()).isEqualByComparingTo(new BigDecimal("100.00"));
    }

    @Test
    @DisplayName("A keyed lookup for a remote key makes exactly ONE hop, and it carries local=true")
    void keyedLookupMakesOneTargetedHopWithTheGuard() throws Exception {
        KeyQueryMetadata metadata = new KeyQueryMetadata(PEER_A, Set.of(), 0);
        when(streams.queryMetadataForKey(anyString(), any(),
                any(org.apache.kafka.common.serialization.Serializer.class))).thenReturn(metadata);

        transport.respondJson(StubHttpTransport.hostPort(PEER_A), JSON.writeValueAsString(
                CustomerSpendingResult.builder()
                        .customerId("C1").found(true)
                        .totalSpent(new BigDecimal("100.00")).orderCount(1)
                        .avgOrderValue(new BigDecimal("100.00")).lastOrderTimestamp(1_000L)
                        .servedLocally(true)
                        .build()));

        CustomerSpendingResult result = service.customerSpending("C1", false);

        // O(1) question, O(1) answer: one hop to the single instance that can own this key.
        assertThat(transport.requestedUris()).hasSize(1);
        assertThat(transport.requestedUris().get(0).getQuery()).contains("local=true");
        assertThat(transport.requestedUris().get(0).getPath())
                .isEqualTo("/api/analytics/customer-spending/C1");

        assertThat(result.getServedBy()).isEqualTo("10.0.0.2:8090");
        assertThat(result.isServedLocally()).isFalse();
        assertThat(result.getTotalSpent()).isEqualByComparingTo(new BigDecimal("100.00"));
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Fixtures
    // ═══════════════════════════════════════════════════════════════════

    private void givenStoreHostedBy(String storeName, StreamsMetadata... instances) {
        when(streams.streamsMetadataForStore(storeName)).thenReturn(List.of(instances));
    }

    private StreamsMetadata active(HostInfo host) {
        StreamsMetadata metadata = mock(StreamsMetadata.class);
        when(metadata.hostInfo()).thenReturn(host);
        when(metadata.stateStoreNames()).thenReturn(Set.of(SPENDING_STORE, SALES_STORE));
        when(metadata.standbyStateStoreNames()).thenReturn(Set.of());
        return metadata;
    }

    private StreamsMetadata standbyOnly(HostInfo host) {
        StreamsMetadata metadata = mock(StreamsMetadata.class);
        when(metadata.hostInfo()).thenReturn(host);
        when(metadata.stateStoreNames()).thenReturn(Set.of());
        when(metadata.standbyStateStoreNames()).thenReturn(Set.of(SPENDING_STORE, SALES_STORE));
        return metadata;
    }

    @SuppressWarnings("unchecked")
    private void givenLocalSpendingStore(KeyValue<String, CustomerSpending>... entries) {
        ReadOnlyKeyValueStore<String, CustomerSpending> store = mock(ReadOnlyKeyValueStore.class);
        when(store.all()).thenReturn(new ListKeyValueIterator<>(List.of(entries)));
        doReturn(store).when(streams).store(any(StoreQueryParameters.class));
    }

    @SuppressWarnings("unchecked")
    private void givenLocalSpendingStoreValue(String key, CustomerSpending value) {
        ReadOnlyKeyValueStore<String, CustomerSpending> store = mock(ReadOnlyKeyValueStore.class);
        when(store.get(key)).thenReturn(value);
        doReturn(store).when(streams).store(any(StoreQueryParameters.class));
    }

    private static KeyValue<String, CustomerSpending> spending(String customerId, String total,
                                                               long orderCount, long lastTs) {
        return KeyValue.pair(customerId, spendingValue(customerId, total, orderCount, lastTs));
    }

    private static CustomerSpending spendingValue(String customerId, String total,
                                                  long orderCount, long lastTs) {
        CustomerSpending value = new CustomerSpending();
        value.setCustomerId(customerId);
        value.setTotalSpent(new BigDecimal(total));
        value.setOrderCount(orderCount);
        value.setAvgOrderValue(new BigDecimal(total)
                .divide(BigDecimal.valueOf(orderCount), 2, java.math.RoundingMode.HALF_UP));
        value.setLastOrderTimestamp(lastTs);
        return value;
    }

    private static CustomerSpendingEntry entry(String customerId, String total,
                                               long orderCount, long lastTs) {
        return CustomerSpendingEntry.of(customerId, spendingValue(customerId, total, orderCount, lastTs));
    }

    private static CategorySalesWindow salesWindow(String category, long startMs, long endMs,
                                                   long orderCount, String totalSales) {
        BigDecimal total = new BigDecimal(totalSales);
        return CategorySalesWindow.builder()
                .category(category)
                .windowStart(Instant.ofEpochMilli(startMs).toString())
                .windowEnd(Instant.ofEpochMilli(endMs).toString())
                .windowStartMs(startMs)
                .windowEndMs(endMs)
                .orderCount(orderCount)
                .totalSales(total)
                .totalQuantity((int) orderCount)
                .avgOrderValue(total.divide(BigDecimal.valueOf(orderCount), 2, java.math.RoundingMode.HALF_UP))
                .maxOrderValue(total)
                .minOrderValue(new BigDecimal("1.00"))
                .build();
    }
}
