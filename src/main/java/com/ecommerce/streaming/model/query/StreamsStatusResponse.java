package com.ecommerce.streaming.model.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Response body of {@code GET /api/analytics/streams/status}.
 *
 * <p>Deliberately answers 200 for EVERY {@code KafkaStreams.State} including
 * {@code REBALANCING} and {@code NOT_INITIALIZED}: this is the endpoint you call to find out
 * WHY the query endpoints are returning 503, so it must not itself 503.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamsStatusResponse {

    /** {@code RUNNING}, {@code REBALANCING}, {@code CREATED}, ... or {@code NOT_INITIALIZED}. */
    private String state;

    /** True only when state is {@code RUNNING} — i.e. Interactive Queries will be served. */
    private boolean queryable;

    /** This instance's {@code application.server} value, {@code host:port}. */
    private String localHost;

    /**
     * Every instance of the application discovered through the Streams metadata, with the state
     * stores each one hosts. Empty until the first rebalance completes.
     */
    private List<InstanceInfo> instances;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class InstanceInfo {
        private String host;
        private boolean self;
        /** State store names for which this instance holds an ACTIVE (not standby) task. */
        private List<String> activeStores;
        /** Partition numbers of the active tasks assigned to this instance. */
        private List<String> topicPartitions;
    }
}
