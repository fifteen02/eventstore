package com.fifteen02.eventstore.constants;

/**
 * AMQP header names used by EventStore.
 *
 * <p>Producers set these headers on messages published to the input queue
 * ({@code eventstore.input.queue}); the message body is the JSON content. EventStore sets them on
 * every message it publishes to an interface's {@code <name>.sink} queue. Consumers of acked
 * interfaces copy {@link #SERVER_CORRELATION_UUID} into the header of their acknowledgement.
 *
 * @see com.fifteen02.eventstore.to.AcknowledgeTo
 */
public final class MessageHeaders {

    private MessageHeaders() {
        // Utility class - prevent instantiation
    }

    /**
     * Message type, e.g. {@code OrderPlaced}. Required on input; always set on sink messages.
     */
    public static final String MESSAGE_TYPE = "messageType";

    /**
     * Optional user the message belongs to. Used by SSE user filters and sharding.
     */
    public static final String USER_ID = "userId";

    /**
     * Optional caller correlation id, stored and forwarded as-is.
     */
    public static final String CORRELATION_ID = "correlationId";

    /**
     * Global EventStore index of the message (a {@code long}). Set on sink messages only.
     * Use it as the idempotency key and as the {@code index} of an acknowledgement.
     */
    public static final String MESSAGE_ID = "messageId";

    /**
     * W3C / OpenTelemetry baggage.
     */
    public static final String BAGGAGE = "baggage";

    /**
     * SHA-256 hex digest of the message body, used for duplicate suppression. Set on sink messages only.
     */
    public static final String CONTENT_HASH = "contentHash";

    /**
     * Optional metadata: a JSON object encoded as a string.
     */
    public static final String METADATA = "metadata";

    /**
     * Message timestamp. On input: ISO-8601 or epoch milliseconds; defaults to the ingestion time.
     */
    public static final String TIMESTAMP = "timestamp";

    /**
     * W3C {@code traceparent}.
     */
    public static final String TRACE_PARENT = "traceparent";

    /**
     * W3C {@code tracestate}.
     */
    public static final String TRACE_STATE = "tracestate";

    /**
     * Identifies the current EventStore run. Set on sink messages; must be copied into the header
     * of every acknowledgement. Acknowledgements without a matching header are discarded, which
     * stops acks for deliveries made before a restart from being applied.
     */
    public static final String SERVER_CORRELATION_UUID = "serverCorrelationUUID";
}
