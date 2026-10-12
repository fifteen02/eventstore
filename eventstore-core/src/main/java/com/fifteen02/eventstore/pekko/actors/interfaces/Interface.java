package com.fifteen02.eventstore.pekko.actors.interfaces;

import com.fifteen02.eventstore.to.Index;
import com.fifteen02.eventstore.to.MessageTo;

import java.util.function.Supplier;

public interface Interface {
    sealed interface Command permits Routed, ExternalAck, HeartbeatFailed {
    }

    record Routed(MessageTo messageTo) implements Command {
    }

    record ExternalAck(Index index, Supplier<Void> acknowledgeOnQueue) implements Command {
    }

    record HeartbeatFailed() implements Command {
    }

}
