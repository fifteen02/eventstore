package com.fifteen02;

import com.fifteen02.eventstore.config.InterfaceType;
import com.fifteen02.eventstore.config.InterfacesConfigProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class AmqpInitializerTest {

    @Mock
    private ConnectionFactory connectionFactory;

    @Mock
    private AmqpAdmin amqpAdmin;

    @Test
    void failsStartupWhenAPersisterHasIncludeTypes() {
        var definition = persister();
        definition.setIncludeTypes(Set.of("OrderShipped"));

        assertThatThrownBy(() -> initializer(definition).run(new DefaultApplicationArguments()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Persister interface [archive]")
                .hasMessageContaining("include-types or exclude-types");
        verify(amqpAdmin, never()).declareQueue(any(Queue.class));
    }

    @Test
    void failsStartupWhenAPersisterHasExcludeTypes() {
        var definition = persister();
        definition.setExcludeTypes(Set.of("OrderShipped"));

        assertThatThrownBy(() -> initializer(definition).run(new DefaultApplicationArguments()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Persister interface [archive]");
    }

    @Test
    void acceptsAPersisterWithEmptyTypeFiltersAndAFilteredReplayable() {
        var archive = persister();
        archive.setIncludeTypes(Set.of());
        var replayable = new InterfacesConfigProperties.InterfaceDefinition();
        replayable.setName("orders");
        replayable.setType(InterfaceType.replayable);
        replayable.setIncludeTypes(Set.of("OrderShipped"));
        replayable.setAck(new InterfacesConfigProperties.Ack());

        assertThatCode(() -> initializer(archive, replayable).run(new DefaultApplicationArguments()))
                .doesNotThrowAnyException();
    }

    private AmqpInitializer initializer(InterfacesConfigProperties.InterfaceDefinition... definitions) {
        var properties = new InterfacesConfigProperties();
        properties.getInterfaces().addAll(java.util.List.of(definitions));
        return new AmqpInitializer(properties, connectionFactory, amqpAdmin, new RabbitProperties());
    }

    private InterfacesConfigProperties.InterfaceDefinition persister() {
        var definition = new InterfacesConfigProperties.InterfaceDefinition();
        definition.setName("archive");
        definition.setType(InterfaceType.persister);
        definition.setAck(new InterfacesConfigProperties.Ack());
        return definition;
    }
}
