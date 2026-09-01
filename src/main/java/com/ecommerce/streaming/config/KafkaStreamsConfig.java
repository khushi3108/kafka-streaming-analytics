package com.ecommerce.streaming.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Streams bootstrap configuration.
 *
 * <p>@EnableKafkaStreams activates Spring's Kafka Streams support, which:
 *  1. Creates a StreamsBuilderFactoryBean (manages lifecycle)
 *  2. Makes StreamsBuilder available for injection into topology builders
 *  3. Starts/stops KafkaStreams with the Spring ApplicationContext
 *
 * <p>The DEFAULT_STREAMS_CONFIG_BEAN_NAME bean provides all StreamsConfig properties.
 */
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    /**
     * Primary Kafka Streams configuration bean.
     *
     * @Value moved to METHOD PARAMETERS (not class fields) so Spring guarantees
     * values are resolved before this bean method is invoked — field injection
     * in @Configuration classes has no ordering guarantee relative to @Bean methods.
     */
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.streams.application-id}") String applicationId) {

        Map<String, Object> props = new HashMap<>();

        // ─── Core ─────────────────────────────────────────────────
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // ─── Serdes ───────────────────────────────────────────────
        // Default key/value serdes; topology overrides them with JsonSerde<T> per stream
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // ─── Processing guarantee: EXACTLY-ONCE ───────────────────
        // Previously unset, which meant AT_LEAST_ONCE. On a crash between "records produced"
        // and "offsets committed", the redelivered records were re-aggregated: the windowed
        // category-sales aggregate eventually aged the duplicates out, but the UNWINDOWED
        // customer-spending store has no window to expire them, so the drift there was
        // permanent and grew with every restart.
        //
        // EXACTLY_ONCE_V2 makes each commit a single Kafka transaction spanning
        //   (a) the records written to output topics,
        //   (b) the records written to state-store changelog topics, and
        //   (c) the consumer offsets for the input records that produced them.
        // Either all three land or none do, so a redelivered record cannot double-count.
        // V2 (vs the deprecated EXACTLY_ONCE) uses one producer per StreamThread instead of
        // one per task, which is why running it with NUM_STREAM_THREADS=2 is cheap here.
        // Requires broker >= 2.5 and a replicated __transaction_state log — see docker-compose.yml,
        // which now runs three brokers with RF=3 / min.insync.replicas=2 for exactly this reason.
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // ─── Producer settings backing exactly-once ───────────────
        // EXACTLY_ONCE_V2 already forces these two internally; setting them explicitly makes
        // the contract visible at the config site and fails fast (rather than silently) if
        // anyone ever downgrades the processing guarantee.
        //   enable.idempotence : broker de-duplicates producer retries via (PID, epoch, seq),
        //                        so a retried send after a network timeout is not a second copy.
        //   acks=all           : the leader waits for all in-sync replicas before acknowledging.
        //                        With min.insync.replicas=2 on the brokers, a write survives the
        //                        loss of any single broker. acks=1 would let an acknowledged
        //                        transaction disappear with a failed leader — exactly-once
        //                        semantics on top of losable data is not a guarantee.
        props.put(StreamsConfig.producerPrefix(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), true);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
        // Idempotence caps this at 5; being explicit documents that ordering per partition
        // is preserved despite pipelining.
        props.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION), 5);

        // transaction.timeout.ms — how long the coordinator waits for an open transaction
        // before aborting it. Streams' default is 10s, which is tight: a long RocksDB flush,
        // a GC pause or a slow changelog write during a commit can blow past it and force an
        // unnecessary abort + task restart. 60s gives real headroom while still bounding how
        // long a read_committed consumer can be blocked behind a hung producer (a downstream
        // reader cannot see past an open transaction). Must stay <= the broker's
        // transaction.max.timeout.ms (default 15 min) or the producer is rejected at startup.
        props.put(StreamsConfig.producerPrefix(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG), 60_000);

        // ─── Consumer settings backing exactly-once ───────────────
        // Streams sets this itself under EOS; stated explicitly so it is obvious that this app
        // never reads records belonging to an aborted or still-open transaction.
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.ISOLATION_LEVEL_CONFIG), "read_committed");

        // ─── Internal (changelog / repartition) topic durability ──
        // Streams creates its own changelog and repartition topics. They defaulted to RF=1,
        // which would make the three-broker cluster pointless: losing one broker would lose a
        // state store's changelog and the app could not rebuild state on restart.
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        props.put(StreamsConfig.topicPrefix(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG), "2");

        // ─── Performance & Reliability ────────────────────────────
        // commit.interval.ms directly sets the EOS transaction size, and therefore the
        // end-to-end latency floor: downstream read_committed consumers (ksqlDB, the console
        // consumer, the demo dashboard) cannot see ANY record of a transaction until it
        // commits. At the previous 1000ms, a fraud alert produced 1ms after a commit stayed
        // invisible for ~1s — unacceptable for the one output that exists to be acted on fast.
        // Kafka's own EOS default is 100ms, which is the balance we want: ~10 transactions per
        // second per thread is a negligible marker/coordinator overhead at this volume, and it
        // caps added visibility latency at 100ms. (Raise it toward 500-1000ms if throughput
        // ever matters more than alert latency; the category-sales path is unaffected either
        // way because suppress() already delays it to window close + grace.)
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        // Record cache: 10 MB. Previously 0, which forced Kafka Streams to forward a
        // downstream update for EVERY input record – a 100-order window produced 100
        // messages on `category-sales` instead of one. Combined with the suppress()
        // operator in OrderStreamTopology, each window now emits exactly one final result.
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);

        // ─── Event time ───────────────────────────────────────────
        // Use the payload's `timestamp` field, not the broker append time, so the Java
        // topology windows on the same clock as ksqlDB (TIMESTAMP='timestamp' in init.sql).
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                OrderTimestampExtractor.class);

        // ─── Consumer settings ────────────────────────────────────
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // process from beginning

        // ─── Error handling ───────────────────────────────────────
        // Was LogAndContinueExceptionHandler: a malformed record produced one WARN line and
        // then vanished — no bytes retained, nothing to replay, nothing to audit.
        // DeadLetterDeserializationExceptionHandler writes the raw bytes plus failure metadata
        // to the `dead-letter` topic and then continues.
        //
        // CAVEAT (documented at length on the handler class): that DLQ write uses a separate,
        // NON-transactional producer. It is at-least-once and is not part of the EOS
        // transaction above — a crash between the DLQ write and the offset commit produces a
        // duplicate DLQ entry on restart. Dedupe on (dlq.original.topic, partition, offset).
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                DeadLetterDeserializationExceptionHandler.class);

        // ─── State store ─────────────────────────────────────────
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/ecommerce");

        return new KafkaStreamsConfiguration(props);
    }
}
