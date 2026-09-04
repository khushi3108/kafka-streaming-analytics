package com.ecommerce.streaming.exception;

import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.NoHandlerFoundException;

import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * Single place where every exception escaping a controller becomes a JSON {@link ApiError}.
 *
 * <p>There was previously no {@code @ControllerAdvice} anywhere in this project. That is why a
 * routine Kafka Streams rebalance — which happens on every deploy, scale event and restart —
 * surfaced to callers as an HTTP 500 with a Spring stack trace in the body. Two things were
 * wrong with that: the status code lied about whether retrying would help, and the body leaked
 * internal class names and line numbers to any caller.
 *
 * <p>The status mapping, and why each one:
 * <table>
 *   <tr><td>{@link StreamsNotReadyException}</td>
 *       <td>503 + {@code Retry-After} — expected, transient, retryable.</td></tr>
 *   <tr><td>{@link InvalidStateStoreException}</td>
 *       <td>503 + {@code Retry-After} — the store existed a moment ago and will exist again;
 *           it migrated to another instance or is restoring. Its subclasses
 *           ({@code StateStoreNotAvailableException},
 *           {@code StreamThreadNotStartedException}, ...) are all covered by this one handler
 *           and all mean the same thing operationally.</td></tr>
 *   <tr><td>{@link ResourceNotFoundException}</td><td>404.</td></tr>
 *   <tr><td>bad/unparseable request params</td><td>400 — the caller must change the request.</td></tr>
 *   <tr><td>anything else</td><td>500, with the detail logged and NOT echoed to the caller.</td></tr>
 * </table>
 */
@Slf4j
@RestControllerAdvice
public class ApiExceptionHandler {

    /** Conservative default for stores that are restoring; long enough not to hot-loop a client. */
    private static final int STORE_RETRY_AFTER_SECONDS = 5;

    // ── 503: transient, retryable ────────────────────────────────────────

    @ExceptionHandler(StreamsNotReadyException.class)
    public ResponseEntity<ApiError> handleStreamsNotReady(StreamsNotReadyException ex,
                                                          HttpServletRequest request) {
        // INFO, not ERROR: a rebalance is a normal lifecycle event, and paging on it is noise.
        log.info("503 (streams not ready) for {}: {}", request.getRequestURI(), ex.getMessage());
        return retryable("STREAMS_NOT_READY", ex.getMessage(), ex.getRetryAfterSeconds(), request);
    }

    @ExceptionHandler(InvalidStateStoreException.class)
    public ResponseEntity<ApiError> handleInvalidStateStore(InvalidStateStoreException ex,
                                                            HttpServletRequest request) {
        log.info("503 (state store unavailable) for {}: {}", request.getRequestURI(), ex.getMessage());
        return retryable("STATE_STORE_NOT_AVAILABLE",
                "State store is not available on this instance right now (migrating or restoring). "
                        + "This is expected during a rebalance; retry shortly.",
                STORE_RETRY_AFTER_SECONDS, request);
    }

    // ── 404 ──────────────────────────────────────────────────────────────

    @ExceptionHandler({ResourceNotFoundException.class, NoHandlerFoundException.class})
    public ResponseEntity<ApiError> handleNotFound(Exception ex, HttpServletRequest request) {
        return build(HttpStatus.NOT_FOUND, "NOT_FOUND", ex.getMessage(), null, request);
    }

    // ── 400 ──────────────────────────────────────────────────────────────

    @ExceptionHandler({
            IllegalArgumentException.class,
            DateTimeParseException.class,
            MethodArgumentTypeMismatchException.class,
            MissingServletRequestParameterException.class,
            MethodArgumentNotValidException.class
    })
    public ResponseEntity<ApiError> handleBadRequest(Exception ex, HttpServletRequest request) {
        return build(HttpStatus.BAD_REQUEST, "BAD_REQUEST", ex.getMessage(), null, request);
    }

    // ── 500 fallback ─────────────────────────────────────────────────────

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleUnexpected(Exception ex, HttpServletRequest request) {
        // The stack trace goes to the log, where operators can see it. The caller gets a
        // generic message: leaking internals into an HTTP body helps no client and helps
        // an attacker map the application.
        log.error("Unhandled exception for {}", request.getRequestURI(), ex);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR",
                "Unexpected server error. See server logs for details.", null, request);
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private ResponseEntity<ApiError> retryable(String code, String message,
                                               int retryAfterSeconds, HttpServletRequest request) {
        return build(HttpStatus.SERVICE_UNAVAILABLE, code, message, retryAfterSeconds, request);
    }

    private ResponseEntity<ApiError> build(HttpStatus status, String code, String message,
                                           Integer retryAfterSeconds, HttpServletRequest request) {
        ApiError body = ApiError.builder()
                .status(status.value())
                .error(status.getReasonPhrase())
                .code(code)
                .message(message)
                .path(request.getRequestURI())
                .retryAfterSeconds(retryAfterSeconds)
                .timestamp(Instant.now().toString())
                .build();

        ResponseEntity.BodyBuilder response = ResponseEntity.status(status);
        if (retryAfterSeconds != null) {
            // Standard HTTP retry signalling, so proxies and generic clients back off correctly
            // without having to understand our JSON body.
            response.header(HttpHeaders.RETRY_AFTER, String.valueOf(retryAfterSeconds));
        }
        return response.body(body);
    }
}
