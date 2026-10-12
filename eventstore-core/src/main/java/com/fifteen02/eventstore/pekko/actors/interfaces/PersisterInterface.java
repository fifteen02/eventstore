package com.fifteen02.eventstore.pekko.actors.interfaces;

import com.fifteen02.eventstore.config.InterfacesConfigProperties;
import com.fifteen02.eventstore.pekko.actors.ack.Acknowledger;
import com.fifteen02.eventstore.pekko.actors.ack.PersisterBatchAcknowledger;
import com.fifteen02.eventstore.service.CatchUpPlannerService;
import com.fifteen02.eventstore.service.InterfaceStateService;
import com.fifteen02.eventstore.service.PersistorService;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import tools.jackson.databind.ObjectMapper;

/**
 * A replayable interface whose acknowledged range is recorded as archived, which retention then trusts
 * to delete rows from PostgreSQL. So it never acknowledges a message the archive did not receive: it has
 * no type filters (rejected at startup) and it does not auto-acknowledge while paused.
 */
public class PersisterInterface extends ReplayableInterface {
    private static final Logger log = LoggerFactory.getLogger(PersisterInterface.class);

    private boolean heldWhilePaused;
    private long replayedThrough;

    protected PersisterInterface(ActorContext<Command> context, String serverCorrelationUUID,
                                 InterfacesConfigProperties.InterfaceDefinition interfaceDefinition, InterfaceStateService interfaceStateService,
                                 RabbitTemplate rabbitTemplate, ObjectMapper objectMapper, CatchUpPlannerService catchUpPlannerService,
                                 Acknowledger acknowledger, ConnectionFactory connectionFactory) {
        super(context, serverCorrelationUUID, interfaceDefinition, interfaceStateService, rabbitTemplate, objectMapper,
                catchUpPlannerService, acknowledger, connectionFactory);
    }

    public static Behavior<Command> create(
            String serverCorrelationUUID,
            InterfacesConfigProperties.InterfaceDefinition def,
            InterfaceStateService interfaceStateService,
            PersistorService persistorService,
            RabbitTemplate rabbitTemplate,
            ObjectMapper objectMapper,
            CatchUpPlannerService catchUpPlannerService,
            ConnectionFactory connectionFactory

    ) {
        return Behaviors.setup(ctx -> new PersisterInterface(
                ctx, serverCorrelationUUID, def, interfaceStateService, rabbitTemplate, objectMapper, catchUpPlannerService,
                new PersisterBatchAcknowledger(def.getName(), interfaceStateService, persistorService, def.getAck().getBatch()),
                connectionFactory));
    }

    @Override
    protected Behavior<Command> onRouted(Routed r) {
        if (!interfaceStateService.isRunning(interfaceDefinition.getName())) {
            // Hold the position rather than auto-ack: the saved position must never pass an unarchived id.
            heldWhilePaused = true;
            return this;
        }
        if (heldWhilePaused) {
            // Resend what was skipped while paused from the saved position; this includes the current message.
            heldWhilePaused = false;
            log.info("Persister {} resumed; catching up from its saved position", interfaceDefinition.getName());
            replayedThrough = Math.max(replayedThrough, catchUpIfNeeded());
        }
        if (r.messageTo().id() <= replayedThrough) {
            return this;
        }
        return doPublish(r);
    }
}
