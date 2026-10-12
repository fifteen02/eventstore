package com.fifteen02.eventstore.pekko.actors.interfaces;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

import java.time.Duration;
import java.util.function.LongSupplier;

class StreamingHeartbeatActor extends AbstractBehavior<StreamingHeartbeatActor.Command> {

    sealed interface Command permits Tick {
    }

    private record Tick() implements Command {
    }

    private final StreamingInterfaceMessageWriter messageWriter;
    private final LongSupplier lastPersistedIndexSupplier;
    private final ActorRef<Interface.Command> parent;

    private StreamingHeartbeatActor(ActorContext<Command> context,
                                    StreamingInterfaceMessageWriter messageWriter,
                                    LongSupplier lastPersistedIndexSupplier,
                                    ActorRef<Interface.Command> parent) {
        super(context);
        this.messageWriter = messageWriter;
        this.lastPersistedIndexSupplier = lastPersistedIndexSupplier;
        this.parent = parent;
    }

    static Behavior<Command> create(StreamingInterfaceMessageWriter messageWriter,
                                    LongSupplier lastPersistedIndexSupplier,
                                    ActorRef<Interface.Command> parent,
                                    Duration heartbeatInterval) {
        return Behaviors.withTimers(timers -> {
            timers.startTimerAtFixedRate(new Tick(), heartbeatInterval);
            return Behaviors.setup(ctx -> new StreamingHeartbeatActor(
                    ctx,
                    messageWriter,
                    lastPersistedIndexSupplier,
                    parent));
        });
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Tick.class, this::onTick)
                .build();
    }

    private Behavior<Command> onTick(Tick tick) {
        if (messageWriter.writeHeartbeat(progress())) {
            return this;
        }
        parent.tell(new Interface.HeartbeatFailed());
        return Behaviors.stopped();
    }

    private StreamProgress progress() {
        return new StreamProgress(lastPersistedIndexSupplier.getAsLong());
    }
}
