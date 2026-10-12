package com.fifteen02.eventstore.pekko.actors.interfaces;

import com.fifteen02.eventstore.config.InterfaceType;
import com.fifteen02.eventstore.config.InterfacesConfigProperties;
import com.fifteen02.eventstore.constants.MessageHeaders;
import com.fifteen02.eventstore.entity.Message;
import com.fifteen02.eventstore.pekko.actors.ack.Acknowledger;
import com.fifteen02.eventstore.service.CatchUpPlannerService;
import com.fifteen02.eventstore.service.InterfaceStateService;
import com.fifteen02.eventstore.to.MessageTo;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Drives {@code onRouted} directly with a mocked actor context: the decision under test does not need a
 * running actor, and a real one would also start the ack-queue consumer.
 */
@ExtendWith(MockitoExtension.class)
class PersisterInterfaceTest {

    private static final String NAME = "archive";
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private ActorContext<Interface.Command> context;

    @Mock
    private InterfaceStateService interfaceStateService;

    @Mock
    private RabbitTemplate rabbitTemplate;

    @Mock
    private CatchUpPlannerService catchUpPlannerService;

    @Mock
    private Acknowledger acknowledger;

    @Mock
    private ConnectionFactory connectionFactory;

    @Test
    void pausedPersisterNeitherPublishesNorAcknowledges() {
        var persister = persister();
        when(interfaceStateService.isRunning(NAME)).thenReturn(false);

        persister.onRouted(new Interface.Routed(messageTo(5L)));

        // An ack here would advance persister_state.lastIndex past an id the archive never received.
        verify(acknowledger, never()).ack(any());
        verify(rabbitTemplate, never()).send(any(), any(), any(org.springframework.amqp.core.Message.class));
    }

    @Test
    void resumedPersisterCatchesUpFromItsSavedPositionWithoutResendingCaughtUpMessages() {
        var persister = persister();
        when(interfaceStateService.isRunning(NAME)).thenReturn(false, true, true, true);
        when(catchUpPlannerService.plan(NAME)).thenReturn(Optional.of(new CatchUpPlannerService.Plan(4L, 6L, List.of(
                new CatchUpPlannerService.Segment("realtime", 5L, 6L, () -> List.of(message(5L), message(6L)))))));

        persister.onRouted(new Interface.Routed(messageTo(5L)));
        persister.onRouted(new Interface.Routed(messageTo(6L)));
        persister.onRouted(new Interface.Routed(messageTo(6L)));
        persister.onRouted(new Interface.Routed(messageTo(7L)));

        var sent = ArgumentCaptor.forClass(org.springframework.amqp.core.Message.class);
        verify(rabbitTemplate, times(3)).send(eq(""), eq(NAME + ".sink"), sent.capture());
        assertThat(sent.getAllValues())
                .extracting(m -> m.getMessageProperties().<Long>getHeader(MessageHeaders.MESSAGE_ID))
                .containsExactly(5L, 6L, 7L);
        verify(acknowledger, never()).ack(any());
    }

    @Test
    void pausedReplayableInterfaceStillAutoAcknowledges() {
        var replayable = new ReplayableInterface(context, "server-correlation", definition(InterfaceType.replayable),
                interfaceStateService, rabbitTemplate, objectMapper, catchUpPlannerService, acknowledger, connectionFactory);
        when(interfaceStateService.isRunning(NAME)).thenReturn(false);

        replayable.onRouted(new Interface.Routed(messageTo(5L)));

        var ack = ArgumentCaptor.forClass(Interface.ExternalAck.class);
        verify(acknowledger).ack(ack.capture());
        assertThat(ack.getValue().index().value()).isEqualTo(5L);
    }

    private PersisterInterface persister() {
        return new PersisterInterface(context, "server-correlation", definition(InterfaceType.persister),
                interfaceStateService, rabbitTemplate, objectMapper, catchUpPlannerService, acknowledger, connectionFactory);
    }

    private InterfacesConfigProperties.InterfaceDefinition definition(InterfaceType type) {
        var definition = new InterfacesConfigProperties.InterfaceDefinition();
        definition.setName(NAME);
        definition.setType(type);
        definition.setAck(new InterfacesConfigProperties.Ack());
        return definition;
    }

    private MessageTo messageTo(long id) {
        return MessageTo.from(message(id));
    }

    private Message message(long id) {
        var message = new Message();
        message.setId(id);
        message.setType("OrderShipped");
        message.setContent(objectMapper.createObjectNode().put("id", id));
        message.setTimestamp(Instant.parse("2026-06-03T08:15:00Z"));
        return message;
    }
}
