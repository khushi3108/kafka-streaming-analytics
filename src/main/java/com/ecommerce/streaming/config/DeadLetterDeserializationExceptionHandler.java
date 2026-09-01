package com.ecommerce.streaming.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Deserialization exception handler that routes poison-pill records to a dead-letter topic
 * instead of silently discarding them.
 *
 * <p>The previous configuration used {@link org.apache.kafka.streams.errors.LogAndContinueExceptionHandler},
 * which emitted a single WARN line and dropped the record. The bytes were gone: no way to
 * inspect what the producer actually sent, no way to replay it after a fix, no audit trail.
 * This handler publishes the record's <em>raw</em> key/value bytes to {@value #DEFAULT_DEAD_LETTER_TOPIC}
 * along with failure metadata in record headers (source topic, partition, offset, source
 * timestamp, exception class, exception message, streams application id), then returns
 * {@link DeserializationHandlerResponse#CONTINUE} so the stream thread keeps making progress.
 *
 * <h2>IMPORTANT — the dead-letter write is NOT part of the processing transaction</h2>
 *
 * <p>The rest of this application runs under {@code EXACTLY_ONCE_V2}: the Streams task's
 * output records and its consumer offsets are written inside one Kafka transaction, so they
 * either all land or none do. This handler is invoked <em>outside</em> that machinery. It owns
 * a separate, plain (non-transactional) producer, for two reasons:
 * <ul>
 *   <li>Kafka Streams does not expose the task's transactional producer to a
 *       {@code DeserializationExceptionHandler}, and</li>
 *   <li>even if it did, a record that fails to deserialize is precisely the record whose
 *       processing transaction is about to be abandoned — enrolling the DLQ write in that
 *       transaction would abort the DLQ write too, which defeats the purpose.</li>
 * </ul>
 *
 * <p>The practical consequence, stated plainly rather than papered over:
 * <ul>
 *   <li><b>The DLQ is at-least-once, not exactly-once.</b> If the JVM dies after the DLQ write
 *       is acknowledged but before the Streams transaction that skips past this offset commits,
 *       the record is re-consumed on restart, fails to deserialize again, and is written to the
 *       DLQ a <em>second</em> time. Duplicate DLQ entries are expected. Consumers of
 *       {@value #DEFAULT_DEAD_LETTER_TOPIC} should dedupe on
 *       ({@value #HEADER_TOPIC}, {@value #HEADER_PARTITION}, {@value #HEADER_OFFSET}), which
 *       uniquely identifies the failed source record.</li>
 *   <li><b>An orphaned DLQ entry is possible in the other direction too.</b> A DLQ record can
 *       exist for an offset whose surrounding transaction was aborted and later replayed
 *       successfully (e.g. after a topic's schema was fixed and the data re-produced). A DLQ
 *       entry is evidence that a deserialization attempt failed, not proof that the source
 *       record was permanently lost.</li>
 *   <li><b>A hard failure of the DLQ producer degrades to log-and-continue.</b> If the send
 *       fails, we log an ERROR and still return CONTINUE — blocking the stream thread on a
 *       broken DLQ would convert a single bad record into a full pipeline stall.</li>
 * </ul>
 *
 * <p>The send is followed by an explicit {@link Producer#flush()} so the bytes are acknowledged
 * by the brokers before the stream thread moves on. Poison pills are rare by definition, so the
 * per-record cost of flushing is irrelevant, and it removes the much larger "still sitting in the
 * producer's accumulator when the JVM died" loss window.
 *
 * <p>Instances are created by Kafka Streams once per stream thread, so the producer is shared
 * statically across instances and closed by a JVM shutdown hook.
 */
@Slf4j
public class DeadLetterDeserializationExceptionHandler implements DeserializationExceptionHandler {

    /** Topic that receives records which could not be deserialized. Declared in KafkaTopicConfig. */
    public static final String DEFAULT_DEAD_LETTER_TOPIC = "dead-letter";

    /** Optional StreamsConfig override, e.g. props.put("dead.letter.topic", "my-dlq"). */
    public static final String DEAD_LETTER_TOPIC_CONFIG = "dead.letter.topic";

    // ─── Failure metadata headers ─────────────────────────────────────
    public static final String HEADER_TOPIC          = "dlq.original.topic";
    public static final String HEADER_PARTITION      = "dlq.original.partition";
    public static final String HEADER_OFFSET         = "dlq.original.offset";
    public static final String HEADER_TIMESTAMP      = "dlq.original.timestamp";
    public static final String HEADER_EXCEPTION_CLASS   = "dlq.exception.class";
    public static final String HEADER_EXCEPTION_MESSAGE = "dlq.exception.message";
    public static final String HEADER_APPLICATION_ID = "dlq.application.id";
    public static final String HEADER_FAILED_AT      = "dlq.failed.at";

    /** Shared across stream threads; guarded by the class monitor. */
    private static volatile Producer<byte[], byte[]> producer;

    private String deadLetterTopic = DEFAULT_DEAD_LETTER_TOPIC;
    private String applicationId = "unknown";

    /**
     * Called by Kafka Streams with the full StreamsConfig map, which is where
     * {@code bootstrap.servers} and {@code application.id} come from.
     */
    @Override
    public void configure(Map<String, ?> configs) {
        Object topicOverride = configs.get(DEAD_LETTER_TOPIC_CONFIG);
        if (topicOverride != null) {
            this.deadLetterTopic = topicOverride.toString();
        }
        Object appId = configs.get(StreamsConfig.APPLICATION_ID_CONFIG);
        if (appId != null) {
            this.applicationId = appId.toString();
        }
        initProducer(configs);
    }

    private static synchronized void initProducer(Map<String, ?> configs) {
        if (producer != null) {
            return;
        }
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                configs.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG,
                configs.get(StreamsConfig.APPLICATION_ID_CONFIG) + "-dlq-producer");
        // Durability for the DLQ itself: the audit trail is worthless if it can be lost on a
        // leader failover. acks=all + idempotence means "replicated to min.insync.replicas
        // before the send is acknowledged, and no duplicates from internal producer retries".
        // NOTE: this is *producer-level* idempotence only — it does NOT make the write
        // transactional with respect to the Streams task. See the class Javadoc.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);

        producer = new KafkaProducer<>(props);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Producer<byte[], byte[]> p = producer;
            if (p != null) {
                p.close();
            }
        }, "dlq-producer-shutdown"));
        log.info("Dead-letter producer initialised (bootstrap={})",
                configs.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context,
                                                 ConsumerRecord<byte[], byte[]> record,
                                                 Exception exception) {
        log.error("☠️  Deserialization failed – routing to '{}' : topic={} partition={} offset={} cause={}: {}",
                deadLetterTopic, record.topic(), record.partition(), record.offset(),
                exception.getClass().getName(), exception.getMessage());

        Producer<byte[], byte[]> p = producer;
        if (p == null) {
            // configure() should always have run first; if it somehow did not, degrade to
            // log-and-continue rather than killing the stream thread over a bad record.
            log.error("Dead-letter producer is not initialised – record from {}-{}@{} is LOST",
                    record.topic(), record.partition(), record.offset());
            return DeserializationHandlerResponse.CONTINUE;
        }

        try {
            // Preserve the original timestamp where possible so the DLQ record keeps the
            // source event's position in time; fall back to wall clock for NO_TIMESTAMP.
            long timestamp = record.timestamp() >= 0 ? record.timestamp() : System.currentTimeMillis();

            ProducerRecord<byte[], byte[]> dlqRecord = new ProducerRecord<>(
                    deadLetterTopic,
                    null,                 // let the partitioner choose
                    timestamp,
                    record.key(),         // raw key bytes, exactly as consumed
                    record.value(),       // raw value bytes, exactly as consumed
                    buildHeaders(record, exception));

            p.send(dlqRecord, (metadata, sendException) -> {
                if (sendException != null) {
                    log.error("Failed to write dead-letter record for {}-{}@{} – record is LOST",
                            record.topic(), record.partition(), record.offset(), sendException);
                } else if (log.isDebugEnabled()) {
                    log.debug("Dead-letter record written to {}-{}@{}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
            // Poison pills are rare, so paying for a synchronous flush is cheap insurance:
            // it closes the "still buffered in the accumulator when the JVM died" loss window.
            p.flush();
        } catch (Exception dlqFailure) {
            // Never let DLQ plumbing take down the topology.
            log.error("Dead-letter publishing threw for {}-{}@{} – record is LOST",
                    record.topic(), record.partition(), record.offset(), dlqFailure);
        }

        return DeserializationHandlerResponse.CONTINUE;
    }

    /**
     * Copies the original record's headers and appends the failure metadata.
     * (topic, partition, offset) together uniquely identify the failed source record and are
     * the recommended dedupe key for DLQ consumers — see the class Javadoc.
     */
    private Headers buildHeaders(ConsumerRecord<byte[], byte[]> record, Exception exception) {
        Headers headers = new RecordHeaders();
        for (Header original : record.headers()) {
            headers.add(original);
        }
        headers.add(HEADER_TOPIC, bytes(record.topic()));
        headers.add(HEADER_PARTITION, bytes(Integer.toString(record.partition())));
        headers.add(HEADER_OFFSET, bytes(Long.toString(record.offset())));
        headers.add(HEADER_TIMESTAMP, bytes(Long.toString(record.timestamp())));
        headers.add(HEADER_EXCEPTION_CLASS, bytes(exception.getClass().getName()));
        headers.add(HEADER_EXCEPTION_MESSAGE, bytes(rootMessage(exception)));
        headers.add(HEADER_APPLICATION_ID, bytes(applicationId));
        headers.add(HEADER_FAILED_AT, bytes(Long.toString(System.currentTimeMillis())));
        return headers;
    }

    /** SerializationException usually wraps the interesting Jackson message one level down. */
    private static String rootMessage(Throwable t) {
        Throwable cursor = t;
        String message = String.valueOf(cursor.getMessage());
        while (cursor.getCause() != null && cursor.getCause() != cursor) {
            cursor = cursor.getCause();
            if (cursor.getMessage() != null) {
                message = message + " | caused by " + cursor.getClass().getSimpleName()
                        + ": " + cursor.getMessage();
            }
        }
        return message;
    }

    private static byte[] bytes(String value) {
        return value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
    }
}
