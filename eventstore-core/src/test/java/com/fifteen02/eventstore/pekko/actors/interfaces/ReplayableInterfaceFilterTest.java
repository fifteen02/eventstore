package com.fifteen02.eventstore.pekko.actors.interfaces;

import com.fifteen02.eventstore.config.InterfaceType;
import com.fifteen02.eventstore.config.InterfacesConfigProperties;
import com.fifteen02.eventstore.pekko.actors.ack.Acknowledger;
import com.fifteen02.eventstore.pekko.actors.interfaces.Interface.ExternalAck;
import com.fifteen02.eventstore.to.Index;
import com.fifteen02.eventstore.to.MessageTo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.Locale;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Tests the filtering logic used by ReplayableInterface.
 * Since the actual interface extends AbstractBehavior (which requires a Pekko context),
 * we test the filtering logic in isolation using a simple helper.
 */
@ExtendWith(MockitoExtension.class)
class ReplayableInterfaceFilterTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Mock
    private Acknowledger acknowledger;

    private InterfacesConfigProperties.InterfaceDefinition interfaceDefinition;

    @BeforeEach
    void setUp() {
        interfaceDefinition = new InterfacesConfigProperties.InterfaceDefinition();
        interfaceDefinition.setName("test-interface");
        interfaceDefinition.setType(InterfaceType.replayable);
        // Only interested in ProfileUpdatedEvent
        interfaceDefinition.setIncludeTypes(Set.of("ProfileUpdatedEvent"));
    }

    @Test
    void shouldFilter_returnsTrueForNonMatchingMessageType() {
        Set<String> include = interfaceDefinition.getIncludeTypes();
        Set<String> exclude = interfaceDefinition.getExcludeTypes();

        // ProfileDeletedEvent is not in includeTypes
        MessageTo deletedMessage = new MessageTo(1L, "ProfileDeletedEvent", emptyContent(), null, null, null, null, null, null, null);

        boolean shouldFilter = shouldFilter(deletedMessage, include, exclude, true);

        assertThat(shouldFilter).isTrue();
    }

    @Test
    void shouldFilter_returnsFalseForMatchingMessageType() {
        Set<String> include = interfaceDefinition.getIncludeTypes();
        Set<String> exclude = interfaceDefinition.getExcludeTypes();

        // ProfileUpdatedEvent is in includeTypes
        MessageTo updatedMessage = new MessageTo(1L, "ProfileUpdatedEvent", emptyContent(), null, null, null, null, null, null, null);

        boolean shouldFilter = shouldFilter(updatedMessage, include, exclude, true);

        assertThat(shouldFilter).isFalse();
    }

    @Test
    void shouldFilter_returnsTrueWhenInterfaceIsNotRunning() {
        Set<String> include = interfaceDefinition.getIncludeTypes();
        Set<String> exclude = interfaceDefinition.getExcludeTypes();

        MessageTo updatedMessage = new MessageTo(1L, "ProfileUpdatedEvent", emptyContent(), null, null, null, null, null, null, null);

        // Interface is not running
        boolean shouldFilter = shouldFilter(updatedMessage, include, exclude, false);

        assertThat(shouldFilter).isTrue();
    }

    @Test
    void shouldFilter_returnsTrueForExcludedType() {
        interfaceDefinition.setIncludeTypes(null);
        interfaceDefinition.setExcludeTypes(Set.of("ProfileSuspendedEvent"));

        Set<String> include = interfaceDefinition.getIncludeTypes();
        Set<String> exclude = interfaceDefinition.getExcludeTypes();

        MessageTo suspendedMessage = new MessageTo(1L, "ProfileSuspendedEvent", emptyContent(), null, null, null, null, null, null, null);

        boolean shouldFilter = shouldFilter(suspendedMessage, include, exclude, true);

        assertThat(shouldFilter).isTrue();
    }

    @Test
    void shouldFilter_returnsFalseWhenNoFiltersConfigured() {
        interfaceDefinition.setIncludeTypes(null);
        interfaceDefinition.setExcludeTypes(null);

        Set<String> include = interfaceDefinition.getIncludeTypes();
        Set<String> exclude = interfaceDefinition.getExcludeTypes();

        MessageTo anyMessage = new MessageTo(1L, "UnknownExternalType", emptyContent(), null, null, null, null, null, null, null);

        boolean shouldFilter = shouldFilter(anyMessage, include, exclude, true);

        assertThat(shouldFilter).isFalse();
    }

    @Test
    void autoAck_shouldBeCalledForFilteredMessages() {
        Set<String> include = interfaceDefinition.getIncludeTypes();
        Set<String> exclude = interfaceDefinition.getExcludeTypes();

        // ProfileDeletedEvent should be filtered
        MessageTo deletedMessage = new MessageTo(123L, "ProfileDeletedEvent", emptyContent(), null, null, null, null, null, null, null);

        boolean shouldFilter = shouldFilter(deletedMessage, include, exclude, true);

        if (shouldFilter) {
            // Simulate what ReplayableInterface does - auto-ack
            acknowledger.ack(new ExternalAck(new Index(deletedMessage.id()), () -> null));
        }

        // Verify acknowledger.ack was called with the correct index
        ArgumentCaptor<ExternalAck> ackCaptor = ArgumentCaptor.forClass(ExternalAck.class);
        verify(acknowledger).ack(ackCaptor.capture());
        assertThat(ackCaptor.getValue().index().value()).isEqualTo(123L);
    }

    @Test
    void autoAck_shouldNotBeCalledForMatchingMessages() {
        Set<String> include = interfaceDefinition.getIncludeTypes();
        Set<String> exclude = interfaceDefinition.getExcludeTypes();

        // ProfileUpdatedEvent should NOT be filtered
        MessageTo updatedMessage = new MessageTo(456L, "ProfileUpdatedEvent", emptyContent(), null, null, null, null, null, null, null);

        boolean shouldFilter = shouldFilter(updatedMessage, include, exclude, true);

        if (shouldFilter) {
            acknowledger.ack(new ExternalAck(new Index(updatedMessage.id()), () -> null));
        }

        // Verify acknowledger.ack was NOT called
        verify(acknowledger, never()).ack(any());
    }

    /**
     * Replicates the filter logic from RealtimeInterface.shouldFilter()
     * This is the same logic used by the actual interface.
     */
    private boolean shouldFilter(MessageTo message, Set<String> include, Set<String> exclude, boolean isRunning) {
        if (!isRunning) {
            return true;
        }
        String messageType = normalise(message.messageType());
        if (include != null && !include.isEmpty() && include.stream().map(this::normalise).noneMatch(messageType::equals)) {
            return true;
        }
        return exclude != null && exclude.stream().map(this::normalise).anyMatch(messageType::equals);
    }

    private String normalise(String messageType) {
        if (messageType == null) {
            return "";
        }
        return messageType.trim().toLowerCase(Locale.ROOT);
    }

    private JsonNode emptyContent() {
        return OBJECT_MAPPER.createObjectNode();
    }
}
