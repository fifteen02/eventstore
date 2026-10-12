package com.fifteen02.eventstore.pekko.actors;

import com.fifteen02.eventstore.config.InterfaceType;
import com.fifteen02.eventstore.config.InterfacesConfigProperties;
import com.fifteen02.eventstore.pekko.actors.interfaces.Interface;
import com.fifteen02.eventstore.pekko.actors.interfaces.RealtimeInterface;
import com.fifteen02.eventstore.service.InterfaceStateService;
import com.fifteen02.eventstore.to.MessageTo;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import tools.jackson.databind.ObjectMapper;

import java.net.ConnectException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InterfaceSupervisionTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private InterfaceStateService interfaceStateService;

    @Mock
    private RabbitTemplate rabbitTemplate;

    @Test
    void interfaceKeepsPublishingAfterAFailedPublish() throws Exception {
        var definition = new InterfacesConfigProperties.InterfaceDefinition();
        definition.setName("notifications");
        definition.setType(InterfaceType.realtime);
        var published = new CountDownLatch(1);

        when(interfaceStateService.isRunning("notifications")).thenReturn(true);
        // The broker is down for the first publish and back for the second.
        doThrow(new AmqpConnectException(new ConnectException("broker unreachable")))
                .doAnswer(_ -> {
                    published.countDown();
                    return null;
                })
                .when(rabbitTemplate).send(eq(""), eq("notifications.sink"), any(Message.class));

        ActorSystem<Void> system = ActorSystem.create(Behaviors.setup(ctx -> {
            var ref = ctx.spawn(RouterActor.supervised(RealtimeInterface.create(
                    "server-correlation", definition, interfaceStateService, rabbitTemplate, objectMapper)),
                    "realtime-notifications");
            ref.tell(new Interface.Routed(message(1L)));
            ref.tell(new Interface.Routed(message(2L)));
            return Behaviors.empty();
        }), "interface-supervision-test-" + UUID.randomUUID());

        try {
            // Without supervision the first failure stops the actor and the second message is never sent.
            assertThat(published.await(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            system.terminate();
        }
    }

    private MessageTo message(long id) throws Exception {
        return new MessageTo(id, "OrderShipped", objectMapper.readTree("{\"orderId\":\"o-" + id + "\"}"),
                null, null, null, "customer-1", null, null, "hash-" + id);
    }
}
