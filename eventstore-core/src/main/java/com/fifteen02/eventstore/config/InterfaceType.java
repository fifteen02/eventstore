package com.fifteen02.eventstore.config;

/**
 * Delivery mode of an AMQP interface. See {@code docs/concepts.md}.
 */
public enum InterfaceType {
    /** Publish live messages to {@code <name>.sink}; no acknowledgements, no replay. */
    realtime,
    /** Publish to {@code <name>.sink}, track acknowledgements from {@code <name>.ack}, replay after restart. */
    replayable,
    /** A replayable interface whose acknowledged range marks messages as archived (see {@code docs/operations.md}). */
    persister
}
