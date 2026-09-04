package com.ecommerce.streaming.support;

import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.mock.http.client.MockClientHttpRequest;
import org.springframework.mock.http.client.MockClientHttpResponse;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link ClientHttpRequestFactory} that answers from a canned routing table instead of a
 * socket, so the Interactive Query fan-out can be tested with a REAL {@link
 * org.springframework.web.client.RestClient} and no network, no ports and no server.
 *
 * <p>It also records every URI requested. That is what makes two of the assertions possible at
 * all: "a standby-only host was never contacted" and "a {@code local=true} query issued no
 * outbound RPC" are claims about calls that must NOT happen, and the only way to check them is
 * to look at what the transport saw.
 */
public final class StubHttpTransport implements ClientHttpRequestFactory {

    /** One canned answer, keyed by the peer's {@code host:port}. */
    private record Canned(HttpStatusCode status, String body) {}

    private final Map<String, Canned> routes = new LinkedHashMap<>();
    private final List<URI> requested = new ArrayList<>();

    /** Answer {@code 200 application/json} with {@code json} for any request to {@code hostPort}. */
    public StubHttpTransport respondJson(String hostPort, String json) {
        routes.put(hostPort, new Canned(HttpStatus.OK, json));
        return this;
    }

    /** Answer with a bare status (no body) — used to simulate a peer that is failing. */
    public StubHttpTransport respondStatus(String hostPort, HttpStatus status) {
        routes.put(hostPort, new Canned(status, ""));
        return this;
    }

    /** Every URI the client actually asked for, in order. Empty means no RPC was issued. */
    public List<URI> requestedUris() {
        return List.copyOf(requested);
    }

    public List<String> requestedHosts() {
        return requested.stream().map(uri -> uri.getHost() + ":" + uri.getPort()).toList();
    }

    @Override
    public ClientHttpRequest createRequest(URI uri, HttpMethod httpMethod) {
        requested.add(uri);

        MockClientHttpRequest request = new MockClientHttpRequest(httpMethod, uri);
        Canned canned = routes.get(uri.getHost() + ":" + uri.getPort());

        // An unrouted host answers 502 rather than throwing: the service is supposed to DEGRADE
        // on a peer failure, and a thrown AssertionError here would be swallowed into a
        // QueryFailure and hide the real problem. Tests assert on requestedUris() instead.
        Canned effective = canned != null ? canned : new Canned(HttpStatus.BAD_GATEWAY, "");

        MockClientHttpResponse response =
                new MockClientHttpResponse(effective.body().getBytes(StandardCharsets.UTF_8),
                        effective.status());
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        request.setResponse(response);
        return request;
    }

    /** Convenience for building {@code host:port} keys from the {@code HostInfo} in a test. */
    public static String hostPort(org.apache.kafka.streams.state.HostInfo host) {
        return host.host() + ":" + host.port();
    }
}
