package com.ecommerce.streaming.exception;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Uniform JSON error body for every non-2xx response from this application.
 *
 * <p>Before this existed, an error meant Spring's default HTML/whitelabel body or a raw stack
 * trace, depending on the accept header — neither of which a client can branch on.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApiError {

    private int status;
    /** HTTP reason phrase, e.g. {@code Service Unavailable}. */
    private String error;
    /** Stable machine-readable code, e.g. {@code STREAMS_NOT_READY}, {@code BAD_REQUEST}. */
    private String code;
    private String message;
    /** Request path that produced the error. */
    private String path;
    /** Present on 503: seconds to wait before retrying. Mirrors the {@code Retry-After} header. */
    private Integer retryAfterSeconds;
    private String timestamp;
}
