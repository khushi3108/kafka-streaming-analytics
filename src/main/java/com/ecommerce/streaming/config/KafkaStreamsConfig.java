package com.ecommerce.streaming.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
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

        // ─── Performance & Reliability ────────────────────────────
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);      // commit offsets every 1s
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);  // disable record cache (for demo – see results immediately)
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);

        // ─── Consumer settings ────────────────────────────────────
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // process from beginning

        // ─── Error handling ───────────────────────────────────────
        // Log and skip bad records instead of crashing the stream
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);

        // ─── State store ─────────────────────────────────────────
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/ecommerce");

        return new KafkaStreamsConfiguration(props);
    }
}
