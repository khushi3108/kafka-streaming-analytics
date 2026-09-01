package com.ecommerce.streaming.config;

import com.ecommerce.streaming.model.EnrichedOrder;
import com.ecommerce.streaming.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * EVENT-TIME extractor: reads the {@code timestamp} field out of the record payload
 * instead of using the broker's append time.
 *
 * <p><b>Why this exists.</b> Without a custom extractor Kafka Streams falls back to
 * {@code FailOnInvalidTimestamp}, which uses the <i>record</i> timestamp (producer
 * append time). ksqlDB, however, already declares {@code TIMESTAMP = 'timestamp'} in
 * {@code ksql/init.sql} and therefore windows on the payload's event time. The two
 * engines were bucketing the same order into different minutes, so the Java windowed
 * aggregates and the ksqlDB tables disagreed. This extractor makes the Java topology
 * agree with ksqlDB.
 *
 * <p><b>Fallback policy.</b> This extractor never throws. A malformed or missing
 * payload timestamp degrades to the record timestamp, then to the current partition
 * time — a bad record must not kill the StreamThread. Returning a negative value
 * would make Kafka Streams throw, so the result is always clamped to a valid value.
 */
@Slf4j
public class OrderTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        long eventTime = payloadTimestamp(record.value());

        if (eventTime > 0) {
            return eventTime;
        }

        // ── Fallback 1: broker/producer record timestamp ──────────────
        long recordTime = record.timestamp();
        if (recordTime > 0) {
            log.debug("No usable payload timestamp on topic [{}] – falling back to record timestamp {}",
                    record.topic(), recordTime);
            return recordTime;
        }

        // ── Fallback 2: highest timestamp seen so far on this partition ──
        if (partitionTime >= 0) {
            log.warn("No usable payload or record timestamp on topic [{}] – falling back to partition time {}",
                    record.topic(), partitionTime);
            return partitionTime;
        }

        // ── Fallback 3: wall clock (never return a negative timestamp) ──
        log.warn("No usable timestamp at all on topic [{}] – falling back to wall-clock time", record.topic());
        return System.currentTimeMillis();
    }

    /**
     * Pull the event-time field out of the known payload types.
     * Anything else (e.g. the {@code products} KTable, which has no event time)
     * returns 0 so the caller falls back to the record timestamp.
     */
    private long payloadTimestamp(Object value) {
        if (value instanceof Order order) {
            return order.getTimestamp();
        }
        if (value instanceof EnrichedOrder enriched) {
            return enriched.getTimestamp();
        }
        return 0L;
    }
}
