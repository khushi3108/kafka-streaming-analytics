package com.ecommerce.streaming.exception;

/**
 * Thrown when Kafka Streams cannot serve an Interactive Query <em>right now</em>, but is
 * expected to be able to shortly: the client is not initialized yet, the instance is
 * REBALANCING, or a state store is still restoring from its changelog.
 *
 * <p><b>Why this is not an IllegalStateException.</b> The controller used to throw a raw
 * {@code IllegalStateException} for exactly this case. With no {@code @ControllerAdvice}
 * anywhere in the project, Spring rendered it as a 500 with a stack trace — telling every
 * caller, every load balancer and every uptime monitor that the service had failed.
 *
 * <p>A rebalance is not a failure. It happens on every deploy, every scale-up, every scale-down
 * and every instance restart, it resolves on its own in seconds, and the correct response is
 * {@code 503 Service Unavailable} with a {@code Retry-After} header — a status clients and
 * proxies already know how to retry. 500 says "this request will never work"; 503 says
 * "try again in {@link #getRetryAfterSeconds()} seconds", which is the truth.
 */
public class StreamsNotReadyException extends RuntimeException {

    private static final int DEFAULT_RETRY_AFTER_SECONDS = 5;

    private final int retryAfterSeconds;

    public StreamsNotReadyException(String message) {
        this(message, DEFAULT_RETRY_AFTER_SECONDS, null);
    }

    public StreamsNotReadyException(String message, Throwable cause) {
        this(message, DEFAULT_RETRY_AFTER_SECONDS, cause);
    }

    public StreamsNotReadyException(String message, int retryAfterSeconds, Throwable cause) {
        super(message, cause);
        this.retryAfterSeconds = retryAfterSeconds;
    }

    public int getRetryAfterSeconds() {
        return retryAfterSeconds;
    }
}
