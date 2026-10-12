package com.fifteen02.eventstore.service;

/**
 * Input that can never be stored, whatever the retry. REST answers {@code 400}; AMQP rejects the
 * delivery without requeue.
 */
public class InvalidMessageException extends IllegalArgumentException {

    public InvalidMessageException(String message) {
        super(message);
    }
}
