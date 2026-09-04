package com.ecommerce.streaming.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;

/**
 * HTTP client used for the Interactive Query RPC leg — one instance asking another instance
 * for the partitions it owns.
 *
 * <p>Uses {@link RestClient}, which is already on the classpath via
 * {@code spring-boot-starter-web} (Spring Framework 6.1). No new dependency, no reactive stack
 * dragged in for what is a handful of blocking GETs.
 *
 * <p><b>Timeouts are the whole point of this bean.</b> The fan-out is only as fast as its
 * slowest peer, and the default {@code SimpleClientHttpRequestFactory} has NO timeouts at all —
 * a single instance that has stopped responding (paused JVM, half-open socket after a node
 * loss) would hang the coordinating request forever, and with it the servlet thread serving the
 * client. Bounded timeouts turn "one sick instance" into "a partial result that says it is
 * partial", which is the failure mode this API is built around.
 */
@Configuration
public class InteractiveQueryClientConfig {

    /**
     * @param connectTimeoutMs how long to wait for the TCP connect. Short: peers are on the same
     *                         network and a slow connect means the peer is gone, not busy.
     * @param readTimeoutMs    how long to wait for the response body. Longer, because a peer may
     *                         legitimately be scanning a large RocksDB range.
     */
    @Bean
    public RestClient interactiveQueryRestClient(
            RestClient.Builder builder,
            @Value("${app.streams.rpc.connect-timeout-ms:1000}") long connectTimeoutMs,
            @Value("${app.streams.rpc.read-timeout-ms:3000}") long readTimeoutMs) {

        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofMillis(connectTimeoutMs));
        requestFactory.setReadTimeout(Duration.ofMillis(readTimeoutMs));

        return builder.requestFactory(requestFactory).build();
    }
}
