package com.ecommerce.streaming.service;

import com.ecommerce.streaming.exception.StreamsNotReadyException;
import com.ecommerce.streaming.model.CategorySales;
import com.ecommerce.streaming.model.CustomerSpending;
import com.ecommerce.streaming.model.query.CategorySalesResponse;
import com.ecommerce.streaming.model.query.CategorySalesWindow;
import com.ecommerce.streaming.model.query.CustomerSpendingEntry;
import com.ecommerce.streaming.model.query.CustomerSpendingResponse;
import com.ecommerce.streaming.model.query.CustomerSpendingResult;
import com.ecommerce.streaming.model.query.QueryFailure;
import com.ecommerce.streaming.model.query.StreamsStatusResponse;
import com.ecommerce.streaming.streams.OrderStreamTopology;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * ═══════════════════════════════════════════════════════════════════════════
 *  MULTI-INSTANCE INTERACTIVE QUERIES
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * <h2>The bug this class exists to fix</h2>
 *
 * A Kafka Streams state store is <em>sharded by partition</em>. Each instance of the
 * application is assigned a subset of the input partitions and materialises the state for
 * <em>those partitions only</em>. {@code store.all()} and {@code store.fetchAll(from, to)}
 * therefore iterate ONE INSTANCE'S SLICE of the data — there is no such thing as a
 * cluster-wide iterator.
 *
 * <p>The previous {@code AnalyticsController} called those methods directly and returned the
 * result as if it were the whole answer. With a single instance owning every partition that
 * looks perfectly correct, which is exactly what made it dangerous: bring up a second instance
 * and each one owns roughly half the partitions, so {@code GET /customer-spending} returns
 * roughly half the customers — with HTTP 200, no error, no warning, no indication that anything
 * is missing. A silently wrong answer is worse than a crash, because a crash is detectable and
 * this is not. It would have been discovered the first time someone scaled the deployment to
 * two pods and wondered why the dashboard numbers halved.
 *
 * <p>On top of that, {@code StreamsConfig.APPLICATION_SERVER_CONFIG} was never set, so the
 * instances had no way to even discover each other: {@code streamsMetadataForStore} would have
 * reported {@code HostInfo("unavailable", -1)} for every one of them. The fix therefore has two
 * halves — advertise an address (see {@code KafkaStreamsConfig}), then use it (this class).
 *
 * <h2>Two query shapes, two strategies</h2>
 *
 * <dl>
 *   <dt>KEYED lookup — one customerId</dt>
 *   <dd>{@link KafkaStreams#queryMetadataForKey} runs the key through the same partitioner the
 *       producer used and names the SINGLE instance that owns it. Serve locally if that is us,
 *       otherwise one HTTP call to that host. Never a fan-out: an O(1) question deserves an
 *       O(1) answer.</dd>
 *
 *   <dt>UNKEYED scan — {@code all()} / {@code fetchAll()}</dt>
 *   <dd>{@link KafkaStreams#streamsMetadataForStore} lists every instance hosting the store.
 *       Query all of them IN PARALLEL and merge. Sequential fan-out would make latency
 *       proportional to instance count, which turns scaling out into a slowdown.</dd>
 * </dl>
 *
 * <h2>Recursion guard</h2>
 *
 * The remote leg calls the SAME public endpoints with {@code local=true}, which makes the
 * instance read only its own partitions and skip discovery entirely. Without that flag every
 * instance would fan out to every other instance, which would fan out again — an infinite
 * recursion that saturates the cluster on the first request.
 *
 * <h2>Partial results are reported as partial</h2>
 *
 * Every failure mode below downgrades the answer rather than corrupting it:
 * <ul>
 *   <li>{@link KeyQueryMetadata#NOT_AVAILABLE} — assignment unknown mid-rebalance → 503.</li>
 *   <li>{@link InvalidStateStoreException} (and its subclasses, e.g.
 *       {@code StateStoreNotAvailableException}) — an instance still restoring from its
 *       changelog → that instance is recorded as a {@link QueryFailure}.</li>
 *   <li>RPC timeout / connection refused / non-2xx from a peer → recorded as a
 *       {@link QueryFailure}.</li>
 * </ul>
 * When any host is dropped, {@code partial=true} is set on the response and the affected hosts
 * are named. If EVERY host fails there is no answer worth returning, so the request 503s rather
 * than handing back an empty list that looks like "no data".
 */
@Slf4j
@Service
public class InteractiveQueryService {

    /**
     * Stateless and thread-safe, so one shared instance is fine. It must be the SAME serializer
     * the topology keys with ({@code Serdes.String()}), otherwise the partition this computes is
     * not the partition the record actually landed in and the lookup goes to the wrong host.
     */
    private static final Serializer<String> KEY_SERIALIZER = Serdes.String().serializer();

    private static final String CUSTOMER_SPENDING_PATH = "/api/analytics/customer-spending";
    private static final String CATEGORY_SALES_PATH    = "/api/analytics/category-sales";

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    private final RestClient restClient;

    /** This instance's advertised {@code application.server}; the identity used to detect "me". */
    private final HostInfo localHostInfo;
    private final String localHost;

    /** Scheme for peer RPC. Plain http by default — this is an internal, cluster-local hop. */
    private final String rpcScheme;

    /** Overall budget for one fan-out, across ALL peers, not per peer. */
    private final Duration fanOutTimeout;

    /**
     * One virtual thread per remote call. Virtual threads suit this precisely: the work is
     * blocking I/O against a handful of peers, the count scales with the size of the
     * deployment, and sizing a platform-thread pool for it would either cap the fan-out or
     * waste threads. Nothing here is CPU-bound.
     */
    private final ExecutorService fanOutExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public InteractiveQueryService(StreamsBuilderFactoryBean streamsBuilderFactoryBean,
                                   RestClient restClient,
                                   HostInfo applicationServerHostInfo,
                                   @Value("${app.streams.rpc.scheme:http}") String rpcScheme,
                                   @Value("${app.streams.rpc.fan-out-timeout-ms:5000}") long fanOutTimeoutMs) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
        this.restClient = restClient;
        this.localHostInfo = applicationServerHostInfo;
        this.localHost = hostString(applicationServerHostInfo);
        this.rpcScheme = rpcScheme;
        this.fanOutTimeout = Duration.ofMillis(fanOutTimeoutMs);
        log.info("Interactive Queries: this instance advertises itself as {}", localHost);
    }

    @PreDestroy
    void shutdown() {
        fanOutExecutor.shutdownNow();
    }

    // ═══════════════════════════════════════════════════════════════════
    //  CUSTOMER SPENDING — keyed lookup
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Spending for ONE customer, from wherever in the cluster that customer's partition lives.
     *
     * @param localOnly when true, answer from this instance's partitions only and do not
     *                  consult metadata or call peers. This is the leg a peer invokes; it is
     *                  what stops the fan-out from recursing.
     */
    public CustomerSpendingResult customerSpending(String customerId, boolean localOnly) {
        KafkaStreams streams = requireRunningStreams();

        if (localOnly) {
            return readLocalCustomerSpending(streams, customerId);
        }

        KeyQueryMetadata metadata;
        try {
            metadata = streams.queryMetadataForKey(
                    OrderStreamTopology.CUSTOMER_SPENDING_STORE, customerId, KEY_SERIALIZER);
        } catch (IllegalStateException e) {
            throw new StreamsNotReadyException(
                    "Key metadata is not available yet (client not running). Retry shortly.", e);
        }

        // NOT_AVAILABLE means "the partition assignment is in flux" — the instance genuinely
        // does not know who owns this key yet. Guessing (e.g. answering from the local store)
        // would be the same silent-wrong-answer failure this class exists to remove.
        if (metadata == null || KeyQueryMetadata.NOT_AVAILABLE.equals(metadata)) {
            throw new StreamsNotReadyException(
                    "Partition ownership for customerId '" + customerId + "' is not available yet "
                            + "(rebalance in progress). Retry shortly.");
        }

        HostInfo active = metadata.activeHost();
        if (isUnavailable(active)) {
            throw new StreamsNotReadyException(
                    "The instance owning customerId '" + customerId + "' has not advertised an "
                            + "application.server address yet. Retry shortly.");
        }

        if (isLocal(active)) {
            return readLocalCustomerSpending(streams, customerId);
        }

        // Exactly one remote hop, to the one instance that can possibly have this key.
        try {
            CustomerSpendingResult remote = restClient.get()
                    .uri(peerUri(active, CUSTOMER_SPENDING_PATH + "/" + customerId, Map.of("local", "true")))
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .body(CustomerSpendingResult.class);

            if (remote == null) {
                throw new StreamsNotReadyException(
                        "Empty response from owning instance " + hostString(active) + ". Retry shortly.");
            }
            remote.setServedBy(hostString(active));
            remote.setServedLocally(false);
            return remote;
        } catch (RestClientException e) {
            // The owner is unreachable. There is no second copy to fall back on (standbys are
            // not enabled), so this is a transient inability to answer, not a client error.
            throw new StreamsNotReadyException(
                    "Instance " + hostString(active) + " owns customerId '" + customerId
                            + "' but could not be reached: " + e.getMessage(), e);
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  CUSTOMER SPENDING — unkeyed scan (fan-out)
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Every customer in the WHOLE application, merged across every instance.
     *
     * @param localOnly when true, return only this instance's partitions (the peer RPC leg).
     */
    public CustomerSpendingResponse allCustomerSpending(boolean localOnly) {
        KafkaStreams streams = requireRunningStreams();

        if (localOnly) {
            return readLocalCustomerSpendingScan(streams);
        }

        FanOut<CustomerSpendingResponse> fanOut = fanOut(
                streams,
                OrderStreamTopology.CUSTOMER_SPENDING_STORE,
                () -> readLocalCustomerSpendingScan(streams),
                host -> restClient.get()
                        .uri(peerUri(host, CUSTOMER_SPENDING_PATH, Map.of("local", "true")))
                        .accept(MediaType.APPLICATION_JSON)
                        .retrieve()
                        .body(CustomerSpendingResponse.class));

        // Merge by customerId. Under normal partitioning each customer appears in exactly one
        // shard, so this is usually a concatenation — but it SUMS rather than picking a winner,
        // because a scan that overlaps a rebalance can legitimately see the same key twice and
        // "pick one" is how the old category-sales code lost data.
        Map<String, CustomerSpendingEntry> merged = new LinkedHashMap<>();
        for (CustomerSpendingResponse shard : fanOut.results()) {
            if (shard == null || shard.getCustomers() == null) continue;
            for (CustomerSpendingEntry entry : shard.getCustomers()) {
                if (entry == null || entry.getCustomerId() == null) continue;
                merged.merge(entry.getCustomerId(), entry, CustomerSpendingEntry::merge);
            }
        }

        List<CustomerSpendingEntry> ranked = merged.values().stream()
                .sorted(Comparator.comparing(
                                CustomerSpendingEntry::getTotalSpent,
                                Comparator.nullsFirst(Comparator.naturalOrder()))
                        .reversed()
                        .thenComparing(CustomerSpendingEntry::getCustomerId))
                .toList();

        return CustomerSpendingResponse.builder()
                .fetchedAt(Instant.now().toString())
                .partial(fanOut.partial())
                .hostsQueried(fanOut.hosts())
                .failures(fanOut.failures())
                .localOnly(false)
                .totalCustomers(ranked.size())
                .customers(ranked)
                .build();
    }

    // ═══════════════════════════════════════════════════════════════════
    //  CATEGORY SALES — windowed unkeyed scan (fan-out)
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Windowed category sales across the whole application.
     *
     * <p><b>{@code from}/{@code to} are EVENT time, not wall-clock time.</b> The caller supplies
     * them explicitly; see {@code AnalyticsController} for why the previous implicit
     * {@code Instant.now()} range was a bug.
     */
    public CategorySalesResponse categorySales(Instant from, Instant to, boolean localOnly) {
        KafkaStreams streams = requireRunningStreams();

        if (localOnly) {
            return readLocalCategorySales(streams, from, to);
        }

        FanOut<CategorySalesResponse> fanOut = fanOut(
                streams,
                OrderStreamTopology.CATEGORY_SALES_STORE,
                () -> readLocalCategorySales(streams, from, to),
                host -> restClient.get()
                        .uri(peerUri(host, CATEGORY_SALES_PATH, Map.of(
                                "local", "true",
                                "from", from.toString(),
                                "to", to.toString())))
                        .accept(MediaType.APPLICATION_JSON)
                        .retrieve()
                        .body(CategorySalesResponse.class));

        // Merge on the FULL window identity (category + start + end), never on category alone.
        // Collapsing to the category is precisely the bug being fixed: a 2-minute query spans
        // two 1-minute tumbling windows, and keying by category alone made one of them
        // overwrite the other.
        Map<String, CategorySalesWindow> merged = new LinkedHashMap<>();
        for (CategorySalesResponse shard : fanOut.results()) {
            if (shard == null || shard.getWindows() == null) continue;
            for (CategorySalesWindow window : shard.getWindows()) {
                if (window == null || window.getCategory() == null) continue;
                merged.merge(window.windowKey(), window, CategorySalesWindow::merge);
            }
        }

        List<CategorySalesWindow> windows = merged.values().stream()
                .sorted(Comparator.comparingLong(CategorySalesWindow::getWindowStartMs)
                        .thenComparing(CategorySalesWindow::getCategory))
                .toList();

        return CategorySalesResponse.builder()
                .from(from.toString())
                .to(to.toString())
                .fromMs(from.toEpochMilli())
                .toMs(to.toEpochMilli())
                .fetchedAt(Instant.now().toString())
                .partial(fanOut.partial())
                .hostsQueried(fanOut.hosts())
                .failures(fanOut.failures())
                .localOnly(false)
                .windowCount(windows.size())
                .windows(windows)
                .build();
    }

    // ═══════════════════════════════════════════════════════════════════
    //  STATUS
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Never throws and never 503s — this is the endpoint an operator calls to find out WHY the
     * query endpoints are 503ing, so it has to answer while the client is REBALANCING or absent.
     */
    public StreamsStatusResponse status() {
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        if (streams == null) {
            return StreamsStatusResponse.builder()
                    .state("NOT_INITIALIZED")
                    .queryable(false)
                    .localHost(localHost)
                    .instances(List.of())
                    .build();
        }

        KafkaStreams.State state = streams.state();
        List<StreamsStatusResponse.InstanceInfo> instances = new ArrayList<>();
        try {
            for (StreamsMetadata metadata : streams.metadataForAllStreamsClients()) {
                HostInfo host = metadata.hostInfo();
                instances.add(StreamsStatusResponse.InstanceInfo.builder()
                        .host(hostString(host))
                        .self(isLocal(host))
                        .activeStores(metadata.stateStoreNames().stream().sorted().toList())
                        .topicPartitions(metadata.topicPartitions().stream()
                                .map(TopicPartition::toString).sorted().toList())
                        .build());
            }
        } catch (RuntimeException e) {
            // Metadata is unavailable before the first rebalance completes. Reporting the state
            // without the peer list is still useful; failing the status call is not.
            log.debug("Streams metadata not available while building status: {}", e.getMessage());
        }

        return StreamsStatusResponse.builder()
                .state(state.toString())
                .queryable(state == KafkaStreams.State.RUNNING)
                .localHost(localHost)
                .instances(instances)
                .build();
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Local store reads (this instance's partitions only)
    // ═══════════════════════════════════════════════════════════════════

    private CustomerSpendingResult readLocalCustomerSpending(KafkaStreams streams, String customerId) {
        ReadOnlyKeyValueStore<String, CustomerSpending> store = streams.store(
                StoreQueryParameters.fromNameAndType(
                        OrderStreamTopology.CUSTOMER_SPENDING_STORE,
                        QueryableStoreTypes.<String, CustomerSpending>keyValueStore()));

        // The store value type is CustomerSpending. The old controller declared
        // ReadOnlyKeyValueStore<String, Double> here; generic ERASURE meant that compiled
        // cleanly and blew up with ClassCastException on the first record at runtime, because
        // the cast lives at the call site, not in the store. There is no compiler check that
        // would have caught it — only matching the declared type to the topology does.
        CustomerSpending spending = store.get(customerId);

        if (spending == null) {
            return CustomerSpendingResult.builder()
                    .customerId(customerId)
                    .found(false)
                    .totalSpent(java.math.BigDecimal.ZERO.setScale(2))
                    .orderCount(0)
                    .avgOrderValue(java.math.BigDecimal.ZERO.setScale(2))
                    .servedBy(localHost)
                    .servedLocally(true)
                    .build();
        }

        return CustomerSpendingResult.builder()
                .customerId(customerId)
                .found(true)
                // Straight through as BigDecimal, already scale-2 from CustomerSpending. The
                // old `Math.round(v * 100.0) / 100.0` round-trip through double undid the
                // exactness the BigDecimal migration was for.
                .totalSpent(spending.getTotalSpent())
                .orderCount(spending.getOrderCount())
                .avgOrderValue(spending.getAvgOrderValue())
                .lastOrderTimestamp(spending.getLastOrderTimestamp())
                .servedBy(localHost)
                .servedLocally(true)
                .build();
    }

    private CustomerSpendingResponse readLocalCustomerSpendingScan(KafkaStreams streams) {
        ReadOnlyKeyValueStore<String, CustomerSpending> store = streams.store(
                StoreQueryParameters.fromNameAndType(
                        OrderStreamTopology.CUSTOMER_SPENDING_STORE,
                        QueryableStoreTypes.<String, CustomerSpending>keyValueStore()));

        List<CustomerSpendingEntry> entries = new ArrayList<>();
        try (KeyValueIterator<String, CustomerSpending> iter = store.all()) {
            while (iter.hasNext()) {
                KeyValue<String, CustomerSpending> kv = iter.next();
                if (kv.value == null) continue;
                entries.add(CustomerSpendingEntry.of(kv.key, kv.value));
            }
        }

        return CustomerSpendingResponse.builder()
                .fetchedAt(Instant.now().toString())
                .partial(false)          // complete FOR THIS SHARD; the coordinator decides overall
                .hostsQueried(List.of(localHost))
                .failures(List.of())
                .localOnly(true)
                .totalCustomers(entries.size())
                .customers(entries)
                .build();
    }

    private CategorySalesResponse readLocalCategorySales(KafkaStreams streams, Instant from, Instant to) {
        ReadOnlyWindowStore<String, CategorySales> store = streams.store(
                StoreQueryParameters.fromNameAndType(
                        OrderStreamTopology.CATEGORY_SALES_STORE,
                        QueryableStoreTypes.<String, CategorySales>windowStore()));

        // One entry per (category, window). Nothing is collapsed on the way out.
        Map<String, CategorySalesWindow> windows = new LinkedHashMap<>();
        try (KeyValueIterator<Windowed<String>, CategorySales> iter = store.fetchAll(from, to)) {
            while (iter.hasNext()) {
                KeyValue<Windowed<String>, CategorySales> kv = iter.next();
                if (kv.value == null) continue;
                CategorySalesWindow window = CategorySalesWindow.of(
                        kv.key.key(),
                        kv.key.window().start(),
                        kv.key.window().end(),
                        kv.value);
                // Same (category, window) twice within one store would be a Streams bug, but if
                // it ever happens we SUM rather than discard one of them.
                windows.merge(window.windowKey(), window, CategorySalesWindow::merge);
            }
        }

        List<CategorySalesWindow> sorted = windows.values().stream()
                .sorted(Comparator.comparingLong(CategorySalesWindow::getWindowStartMs)
                        .thenComparing(CategorySalesWindow::getCategory))
                .toList();

        return CategorySalesResponse.builder()
                .from(from.toString())
                .to(to.toString())
                .fromMs(from.toEpochMilli())
                .toMs(to.toEpochMilli())
                .fetchedAt(Instant.now().toString())
                .partial(false)
                .hostsQueried(List.of(localHost))
                .failures(List.of())
                .localOnly(true)
                .windowCount(sorted.size())
                .windows(sorted)
                .build();
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Fan-out machinery
    // ═══════════════════════════════════════════════════════════════════

    /** Outcome of one parallel fan-out: the shards that answered, and the hosts that did not. */
    private record FanOut<R>(List<R> results, List<String> hosts, List<QueryFailure> failures) {
        boolean partial() {
            return !failures.isEmpty();
        }
    }

    /**
     * Query every instance hosting {@code storeName} in parallel and collect the shards.
     *
     * <p>The local read runs on the calling thread WHILE the remote calls are in flight, so a
     * single-instance deployment costs no thread hand-off at all and a multi-instance one
     * overlaps its own RocksDB scan with the network wait.
     *
     * <p>The timeout is a single budget for the whole fan-out, not a per-peer timeout: three
     * dead peers with a 3s timeout each must not add up to a 9s request.
     */
    private <R> FanOut<R> fanOut(KafkaStreams streams,
                                 String storeName,
                                 Supplier<R> localReader,
                                 Function<HostInfo, R> remoteReader) {

        Collection<StreamsMetadata> metadata;
        try {
            metadata = streams.streamsMetadataForStore(storeName);
        } catch (IllegalStateException e) {
            throw new StreamsNotReadyException(
                    "Streams metadata for store '" + storeName + "' is not available yet. Retry shortly.", e);
        }

        List<R> results = Collections.synchronizedList(new ArrayList<>());
        List<QueryFailure> failures = new ArrayList<>();
        Set<String> hosts = new LinkedHashSet<>();

        // Before the first rebalance completes there is no metadata at all. Answering from the
        // local store and calling it complete is exactly the old bug, so this is a 503.
        if (metadata == null || metadata.isEmpty()) {
            throw new StreamsNotReadyException(
                    "No instance is currently hosting store '" + storeName
                            + "' (rebalance in progress or state still restoring). Retry shortly.");
        }

        boolean queryLocal = false;
        Map<HostInfo, CompletableFuture<R>> remoteCalls = new LinkedHashMap<>();

        for (StreamsMetadata instance : metadata) {
            HostInfo host = instance.hostInfo();

            // ACTIVE hosts only. streamsMetadataForStore() also returns instances that hold the
            // store as a STANDBY replica, and a standby is a full COPY of the active's partitions,
            // not a different shard of them. Querying both and merging would therefore count the
            // same records twice — the merge below sums (because shards are disjoint by key), so
            // an included standby would silently DOUBLE every figure it duplicated. That is the
            // same class of quietly-wrong answer this whole class exists to eliminate, just from
            // the opposite direction. Skipping is conservative: only an instance that lists the
            // store as a standby AND not as an active is dropped.
            // (num.standby.replicas is 0 today, so this is a guard for the day it is not.)
            if (!instance.stateStoreNames().contains(storeName)
                    && instance.standbyStateStoreNames().contains(storeName)) {
                log.debug("Skipping standby-only host {} for store '{}'", hostString(host), storeName);
                continue;
            }

            if (isUnavailable(host)) {
                // Only reachable if application.server is unset somewhere in the cluster. Say so
                // instead of pretending the instance does not exist.
                failures.add(QueryFailure.builder()
                        .host("unavailable")
                        .reason("NO_APPLICATION_SERVER")
                        .detail("An instance hosting '" + storeName + "' has not advertised an "
                                + "application.server address, so its partitions cannot be queried.")
                        .build());
                continue;
            }
            hosts.add(hostString(host));
            if (isLocal(host)) {
                queryLocal = true;
            } else {
                remoteCalls.put(host, CompletableFuture.supplyAsync(
                        () -> remoteReader.apply(host), fanOutExecutor));
            }
        }

        long deadlineNanos = System.nanoTime() + fanOutTimeout.toNanos();

        if (queryLocal) {
            try {
                results.add(localReader.get());
            } catch (RuntimeException e) {
                // A local store that is restoring must not fail the whole request when peers can
                // still answer — it degrades the result to partial.
                failures.add(classify(localHost, e));
            }
        }

        for (Map.Entry<HostInfo, CompletableFuture<R>> call : remoteCalls.entrySet()) {
            String host = hostString(call.getKey());
            long remainingNanos = deadlineNanos - System.nanoTime();
            try {
                results.add(call.getValue().get(Math.max(0, remainingNanos), TimeUnit.NANOSECONDS));
            } catch (TimeoutException e) {
                call.getValue().cancel(true);
                failures.add(QueryFailure.builder()
                        .host(host)
                        .reason("TIMEOUT")
                        .detail("Did not answer within the " + fanOutTimeout.toMillis()
                                + "ms fan-out budget.")
                        .build());
            } catch (ExecutionException e) {
                failures.add(classify(host, e.getCause()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failures.add(QueryFailure.builder()
                        .host(host).reason("INTERRUPTED").detail("Query thread was interrupted.")
                        .build());
            }
        }

        // Nothing answered. An empty list with partial=true is technically honest but reads as
        // "there is no data"; 503 says "ask again", which is what is actually true.
        if (results.isEmpty()) {
            throw new StreamsNotReadyException(
                    "No instance could serve store '" + storeName + "'. Failures: " + failures);
        }

        if (!failures.isEmpty()) {
            log.warn("PARTIAL Interactive Query result for store '{}': {} of {} hosts answered. {}",
                    storeName, results.size(), hosts.size(), failures);
        }

        return new FanOut<>(new ArrayList<>(results), new ArrayList<>(hosts), failures);
    }

    /** Turn whatever went wrong into a reason code a client can branch on. */
    private QueryFailure classify(String host, Throwable cause) {
        String reason;
        if (cause instanceof StreamsNotReadyException) {
            reason = "REBALANCING";
        } else if (cause instanceof InvalidStateStoreException) {
            // Covers StateStoreNotAvailableException, UnknownStateStoreException,
            // StreamThreadNotStartedException — all "not now, try again".
            reason = "STORE_NOT_AVAILABLE";
        } else if (cause instanceof RestClientException) {
            reason = "RPC_FAILED";
        } else {
            reason = "QUERY_FAILED";
        }
        log.warn("Interactive Query against {} failed ({}): {}", host, reason,
                cause == null ? "unknown" : cause.toString());
        return QueryFailure.builder()
                .host(host)
                .reason(reason)
                .detail(cause == null ? null : cause.getMessage())
                .build();
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Helpers
    // ═══════════════════════════════════════════════════════════════════

    /**
     * @throws StreamsNotReadyException (→ 503 + Retry-After) when the client is absent or not
     *         RUNNING. This used to be a raw {@code IllegalStateException}, which with no
     *         {@code @ControllerAdvice} in the project rendered as a 500 + stack trace — telling
     *         every monitor that a routine rebalance was a server failure.
     */
    private KafkaStreams requireRunningStreams() {
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        if (streams == null) {
            throw new StreamsNotReadyException(
                    "KafkaStreams is not initialized yet. Retry shortly.");
        }
        KafkaStreams.State state = streams.state();
        if (state != KafkaStreams.State.RUNNING) {
            throw new StreamsNotReadyException(
                    "KafkaStreams is not RUNNING (current state: " + state + "). "
                            + "This is normal during startup and rebalances. Retry shortly.");
        }
        return streams;
    }

    /** Build the URI of a peer's own endpoint, always with {@code local=true}. */
    private URI peerUri(HostInfo host, String path, Map<String, String> queryParams) {
        UriComponentsBuilder builder = UriComponentsBuilder.newInstance()
                .scheme(rpcScheme)
                .host(host.host())
                .port(host.port())
                .path(path);
        queryParams.forEach(builder::queryParam);
        // encode() so a customerId containing reserved characters cannot break out of the path.
        return builder.encode().build().toUri();
    }

    private boolean isLocal(HostInfo host) {
        return localHostInfo.equals(host);
    }

    /**
     * {@code HostInfo.unavailable()} is what Streams reports for an instance that has not set
     * {@code application.server}. Port is also checked because a hand-written config could set
     * the literal host "unavailable" with a real port, or a real host with a negative port.
     */
    private static boolean isUnavailable(HostInfo host) {
        return host == null || HostInfo.unavailable().equals(host) || host.port() < 0;
    }

    private static String hostString(HostInfo host) {
        return host == null ? "unknown" : host.host() + ":" + host.port();
    }
}
