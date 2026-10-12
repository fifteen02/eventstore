package com.fifteen02.eventstore.pekko.actors.interfaces;

import com.fifteen02.eventstore.config.InterfacesConfigProperties;
import com.fifteen02.eventstore.entity.Message;
import com.fifteen02.eventstore.pekko.actors.AckQueueActor;
import com.fifteen02.eventstore.pekko.actors.ack.Acknowledger;
import com.fifteen02.eventstore.pekko.actors.ack.BatchAcknowledger;
import com.fifteen02.eventstore.service.CatchUpPlannerService;
import com.fifteen02.eventstore.service.InterfaceStateService;
import com.fifteen02.eventstore.to.Index;
import com.fifteen02.eventstore.to.MessageTo;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import tools.jackson.databind.ObjectMapper;

public class ReplayableInterface extends RealtimeInterface {
    private static final Logger log = LoggerFactory.getLogger(ReplayableInterface.class);

    private final CatchUpPlannerService catchUpPlannerService;
    private final Acknowledger acknowledger;

    protected ReplayableInterface(ActorContext<Command> context,
                                  String serverCorrelationUUID,
                                  InterfacesConfigProperties.InterfaceDefinition interfaceDefinition,
                                  InterfaceStateService interfaceStateService,
                                  RabbitTemplate rabbitTemplate,
                                  ObjectMapper objectMapper,
                                  CatchUpPlannerService catchUpPlannerService,
                                  Acknowledger acknowledger,
                                  ConnectionFactory connectionFactory

    ) {
        super(context, serverCorrelationUUID, interfaceDefinition, interfaceStateService, rabbitTemplate, objectMapper);

        this.catchUpPlannerService = catchUpPlannerService;
        this.acknowledger = acknowledger;
        interfaceStateService.ensure(interfaceDefinition.getName());

        // Create the acknowledgement queue actor which consumes from <interface>.ack
        context.spawn(
                AckQueueActor.create(
                        serverCorrelationUUID,
                        interfaceDefinition.getName(),
                        interfaceDefinition.getAck().getPrefetch(),
                        connectionFactory,
                        context.getSelf()),
                interfaceDefinition.getName() + "-ack-queue");

        // Perform initial catch-up if there is a lag
        catchUpIfNeeded();
     }

    public static Behavior<Command> create(
            String serverCorrelationUUID,
            InterfacesConfigProperties.InterfaceDefinition def,
            InterfaceStateService interfaceStateService,
            RabbitTemplate rabbitTemplate,
            ObjectMapper objectMapper,
            CatchUpPlannerService catchUpPlannerService,
            ConnectionFactory connectionFactory

    ) {
        return Behaviors.setup(ctx -> new ReplayableInterface(
                ctx, serverCorrelationUUID, def, interfaceStateService, rabbitTemplate, objectMapper, catchUpPlannerService,
                new BatchAcknowledger(def.getName(), interfaceStateService, def.getAck().getBatch()),
                connectionFactory));
    }

    @Override
    protected Behavior<Command> onRouted(Routed r) {
        if (shouldFilter(r)) {
            // Auto-ack filtered messages so the interface pointer stays in sync
            acknowledger.ack(new ExternalAck(new Index(r.messageTo().id()), () -> null));
            return this;
        }
        return doPublish(r);
    }

    @Override
    void publish(Routed r) {
        super.publish(r);
    }

    /**
     * Perform catch-up using planner to determine segments.
     *
     * @return the highest id sent or skipped, or 0 when there was nothing to catch up
     */
    protected long catchUpIfNeeded() {
        try {
            return catchUpPlannerService.plan(interfaceDefinition.getName()).map(plan -> {
                Long lastProcessedMessageId = null;
                int messagesProcessed = 0;
                int messagesSkipped = 0;

                for (CatchUpPlannerService.Segment seg : plan.segments()) {
                    var messages = seg.messages().get();
                    for (Message m : messages) {
                        lastProcessedMessageId = m.getId();

                        if (shouldFilterMessageType(m.getType())) {
                            acknowledger.ack(new ExternalAck(new Index(m.getId()), () -> null));
                            messagesSkipped++;
                            continue;
                        }

                        publish(new Routed(MessageTo.from(m)));
                        messagesProcessed++;
                    }
                }

                if (lastProcessedMessageId != null) {
                    log.info(
                            "Interface {} catch-up completed: processed {} messages, skipped {} messages, reached message ID {}",
                            interfaceDefinition.getName(), messagesProcessed, messagesSkipped, lastProcessedMessageId);
                } else {
                    log.info("Interface {} catch-up completed: no messages to process (already up to date)",
                            interfaceDefinition.getName());
                }
                return lastProcessedMessageId == null ? 0L : lastProcessedMessageId;
            }).orElse(0L);
        } catch (Exception e) {
            // Fail the actor so its supervisor restarts it and the catch-up runs again. Carrying on would
            // leave a gap: live messages would flow while the missed ones were never sent.
            log.error("Interface {} catch-up failed, will retry: {}", interfaceDefinition.getName(), e.getMessage(), e);
            throw e;
        }
    }

    @Override
    protected Behavior<Command> onExternalAck(ExternalAck ack) {
        acknowledger.ack(ack);
        return this;
    }

}
