package com.fifteen02.eventstore.to;

/**
 * Acknowledgement that a consumer of a {@code replayable} or {@code persister} interface has
 * processed a message. Publish it as JSON to the interface's {@code <name>.ack} queue:
 *
 * <pre>{@code {"index": 42, "serverCorrelationUUID": "<from the sink message>"}}</pre>
 *
 * <p>The message must also carry the {@link com.fifteen02.eventstore.constants.MessageHeaders#SERVER_CORRELATION_UUID}
 * <b>header</b> with the value received on the sink message. EventStore checks the header and
 * discards acknowledgements whose header does not match its current run.
 *
 * <p>Acknowledgements may arrive in any order; EventStore advances the interface's position only
 * over contiguous indices.
 *
 * @param index                 the global index, from the sink message's {@code messageId} header
 * @param serverCorrelationUUID the {@code serverCorrelationUUID} header of the sink message
 */
public record AcknowledgeTo(Long index, String serverCorrelationUUID) {

    @Override
    public String toString() {
        return "AcknowledgeTo{index=" + index + ", correlationId='" + serverCorrelationUUID + "'}";
    }
}
