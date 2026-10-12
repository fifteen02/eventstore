package com.fifteen02.eventstore.pekko.actors.interfaces;

import com.fifteen02.eventstore.config.InterfacesConfigProperties;
import com.fifteen02.eventstore.entity.Message;
import com.fifteen02.eventstore.controller.StreamRequestParameters;
import com.fifteen02.eventstore.service.CatchUpPlannerService;
import com.fifteen02.eventstore.to.MessageTo;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Duration;
import java.util.Optional;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

public class StreamingInterface extends AbstractBehavior<Interface.Command> implements Interface {

    private static final Logger log = LoggerFactory.getLogger(StreamingInterface.class);

    private final InterfacesConfigProperties.InterfaceDefinition interfaceDefinition;
    private final CatchUpPlannerService catchUpPlannerService;
    private final StreamRequestParameters requestParameters;
    private final Duration heartbeatInterval;
    private final LongSupplier lastPersistedIndexSupplier;
    private final StreamingInterfaceMessageWriter messageWriter;
    private final LongConsumer onObserved;
    private long lastObservedIndex;

    protected StreamingInterface(ActorContext<Command> context,
                                 InterfacesConfigProperties.InterfaceDefinition interfaceDefinition,
                                 CatchUpPlannerService catchUpPlannerService,
                                 SseEmitter emitter,
                                 StreamRequestParameters requestParameters,
                                 Duration heartbeatInterval,
                                 LongSupplier lastPersistedIndexSupplier,
                                 LongConsumer onObserved) {
        super(context);
        this.onObserved = onObserved;
        this.interfaceDefinition = interfaceDefinition;
        this.catchUpPlannerService = catchUpPlannerService;
        this.requestParameters = requestParameters;
        this.heartbeatInterval = heartbeatInterval;
        this.lastPersistedIndexSupplier = lastPersistedIndexSupplier;
        this.messageWriter = new StreamingInterfaceMessageWriter(
                emitter,
                requestParameters);
        this.lastObservedIndex = requestParameters.startIndex() == null ? 0L : requestParameters.startIndex() - 1L;
    }

    public static Behavior<Command> create(InterfacesConfigProperties.InterfaceDefinition definition,
                                           CatchUpPlannerService catchUpPlannerService,
                                           SseEmitter emitter,
                                           StreamRequestParameters requestParameters,
                                           Duration heartbeatInterval,
                                           LongSupplier lastPersistedIndexSupplier) {
        return create(definition, catchUpPlannerService, emitter, requestParameters, heartbeatInterval,
                lastPersistedIndexSupplier, index -> {
                });
    }

    public static Behavior<Command> create(InterfacesConfigProperties.InterfaceDefinition definition,
                                           CatchUpPlannerService catchUpPlannerService,
                                           SseEmitter emitter,
                                           StreamRequestParameters requestParameters,
                                           Duration heartbeatInterval,
                                           LongSupplier lastPersistedIndexSupplier,
                                           LongConsumer onObserved) {
        return Behaviors.setup(ctx -> new StreamingInterface(
                ctx,
                definition,
                catchUpPlannerService,
                emitter,
                requestParameters,
                heartbeatInterval,
                lastPersistedIndexSupplier,
                onObserved).start());
    }

    private Behavior<Command> start() {
        if (!messageWriter.writeConnected(progress())) {
            return Behaviors.stopped();
        }
        getContext().spawn(
                StreamingHeartbeatActor.create(
                        messageWriter,
                        lastPersistedIndexSupplier,
                        getContext().getSelf(),
                        heartbeatInterval),
                "heartbeat");
        if (catchUpIfNeeded()) {
            return this;
        }
        return Behaviors.stopped();
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Routed.class, this::onRouted)
                .onMessage(ExternalAck.class, this::onExternalAck)
                .onMessage(HeartbeatFailed.class, this::onHeartbeatFailed)
                .build();
    }

    private Behavior<Command> onRouted(Routed routed) {
        writeIfNewer(routed.messageTo());
        return this;
    }

    private Behavior<Command> onExternalAck(ExternalAck externalAck) {
        return this;
    }

    private Behavior<Command> onHeartbeatFailed(HeartbeatFailed heartbeatFailed) {
        return Behaviors.stopped();
    }

    private boolean catchUpIfNeeded() {
        try {
            long requestedStart = requestParameters.startIndex() == null ? 1L : requestParameters.startIndex();
            long fromInclusive = Math.max(1L, requestedStart);
            var result = processCatchUpPlan(planFor(fromInclusive));
            if (!result.streamOpen()) {
                return false;
            }
            if (requestParameters.endIndex() != null) {
                return messageWriter.completeStreamIfEndReached(result.target() == null
                        ? lastPersistedIndexSupplier.getAsLong()
                        : result.target(), progress());
            }
            return true;
        } catch (Exception exception) {
            log.warn("Streaming interface {} catch-up failed: {}",
                    interfaceDefinition.getName(), exception.getMessage());
            messageWriter.completeWithError(exception);
            return false;
        }
    }

    private Optional<CatchUpPlannerService.Plan> planFor(long fromInclusive) {
        if (requestParameters.endIndex() == null) {
            return catchUpPlannerService.planFromStartInclusive(fromInclusive);
        }
        return catchUpPlannerService.planFromStartInclusiveToEndInclusive(fromInclusive, requestParameters.endIndex());
    }

    private CatchUpResult processCatchUpPlan(Optional<CatchUpPlannerService.Plan> maybePlan) {
        if (maybePlan.isEmpty()) {
            return CatchUpResult.empty();
        }

        var plan = maybePlan.orElseThrow();
        Long lastProcessedMessageId = null;
        int messagesProcessed = 0;

        for (CatchUpPlannerService.Segment segment : plan.segments()) {
            var messages = segment.messages().get();
            for (Message message : messages) {
                lastProcessedMessageId = message.getId();
                if (!writeIfNewer(MessageTo.from(message))) {
                    return CatchUpResult.disconnected(plan.target(), lastProcessedMessageId, messagesProcessed);
                }
                messagesProcessed++;
            }
        }
        observe(Math.max(lastObservedIndex, plan.target()));

        if (lastProcessedMessageId != null) {
            log.info(
                    "Streaming interface {} catch-up completed: processed {} messages, reached message ID {}",
                    interfaceDefinition.getName(), messagesProcessed, lastProcessedMessageId);
        } else {
            log.info("Streaming interface {} catch-up completed: no messages to process",
                    interfaceDefinition.getName());
        }

        return new CatchUpResult(plan.target(), lastProcessedMessageId, messagesProcessed, true);
    }

    private boolean writeIfNewer(MessageTo messageTo) {
        if (isBeforeStartIndex(messageTo) || hasAlreadySeen(messageTo)) {
            return true;
        }
        observe(messageTo.id());
        return messageWriter.write(new Routed(messageTo), progress());
    }

    private void observe(long index) {
        lastObservedIndex = index;
        onObserved.accept(index);
    }

    private boolean isBeforeStartIndex(MessageTo messageTo) {
        return requestParameters.startIndex() != null && messageTo.id() < requestParameters.startIndex();
    }

    private boolean hasAlreadySeen(MessageTo messageTo) {
        return messageTo.id() <= lastObservedIndex;
    }

    private StreamProgress progress() {
        long lastPersistedIndex = lastPersistedIndexSupplier.getAsLong();
        return new StreamProgress(lastPersistedIndex);
    }

    private record CatchUpResult(Long target, Long lastProcessedMessageId, int messagesProcessed, boolean streamOpen) {
        private static CatchUpResult empty() {
            return new CatchUpResult(null, null, 0, true);
        }

        private static CatchUpResult disconnected(Long target, Long lastProcessedMessageId, int messagesProcessed) {
            return new CatchUpResult(target, lastProcessedMessageId, messagesProcessed, false);
        }
    }
}

record StreamProgress(long lastPersistedIndex) {
}
