package com.ecommerce.streaming.exception;

/**
 * Thrown when the client asked for something that does not exist — an unknown state store name,
 * for instance. Rendered as a JSON 404 by {@code ApiExceptionHandler}.
 *
 * <p>Note that a customerId with no spending record is NOT this: the key is legitimate, the
 * store simply has no aggregate for it yet. That answers 200 with {@code found: false}, because
 * "this customer has not ordered anything" is a real answer, not a missing resource.
 */
public class ResourceNotFoundException extends RuntimeException {

    public ResourceNotFoundException(String message) {
        super(message);
    }
}
