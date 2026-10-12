package com.fifteen02.eventstore.controller;

import com.fifteen02.eventstore.pekko.ActorRegistry;
import com.fifteen02.eventstore.pekko.actors.RouterActor;
import com.fifteen02.eventstore.service.IndexService;
import com.fifteen02.eventstore.service.OpenStreams;
import com.fifteen02.eventstore.to.Index;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.server.ResponseStatusException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class EventStreamControllerTest {

    private MockMvc mockMvc;

    private EventStreamController controller;
    private final OpenStreams openStreams = new OpenStreams();

    @Mock
    private ActorRegistry actorRegistry;

    @Mock
    private IndexService indexService;

    @BeforeEach
    void setUp() {
        lenient().when(actorRegistry.isReady()).thenReturn(true);
        controller = new EventStreamController(actorRegistry, indexService, openStreams);
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    void stream_returnsServerSentEventsAndRegistersStream() throws Exception {
        lastPersistedIndexIs(100L);

        var mvcResult = mockMvc.perform(get("/api/events/stream")
                        .param("messageType", "ProfileUpdatedEvent", "ExternalSystemEvent")
                        .param("userId", "user-1", "user-2")
                        .param("startIndex", "42"))
                .andExpect(request().asyncStarted())
                .andReturn();

        assertThat(mvcResult.getResponse().getContentType()).startsWith("text/event-stream");
        assertThat(mvcResult.getResponse().getStatus()).isEqualTo(200);

        var commandCaptor = ArgumentCaptor.forClass(RouterActor.Command.class);
        verify(actorRegistry).tellRouter(commandCaptor.capture());
        assertThat(commandCaptor.getValue()).isInstanceOfSatisfying(RouterActor.RegisterSseStream.class, command -> {
            assertThat(command.streamId()).isNotBlank();
            assertThat(command.emitter()).isNotNull();
            assertThat(command.requestParameters().messageType())
                    .containsExactlyInAnyOrder("ProfileUpdatedEvent", "ExternalSystemEvent");
            assertThat(command.requestParameters().userIds())
                    .containsExactlyInAnyOrder("user-1", "user-2");
            assertThat(command.requestParameters().startIndex()).isEqualTo(42L);
            assertThat(command.requestParameters().endIndex()).isNull();
            assertThat(command.requestParameters().shardCount()).isNull();
            assertThat(command.requestParameters().shardIndex()).isNull();
        });
    }

    @Test
    void stream_isRefusedWhileTheRouterIsStarting() throws Exception {
        lastPersistedIndexIs(100L);
        when(actorRegistry.isReady()).thenReturn(false);

        mockMvc.perform(get("/api/events/stream").param("startIndex", "1"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "1"));

        verify(actorRegistry, never()).tellRouter(any());
        assertThat(openStreams.list()).isEmpty();
    }

    @Test
    void stream_isListedAsOpenWithItsClientAndPosition() throws Exception {
        lastPersistedIndexIs(100L);

        mockMvc.perform(get("/api/events/stream")
                        .param("startIndex", "42")
                        .header("User-Agent", "Java-http-client/25.0.1"))
                .andExpect(request().asyncStarted());

        assertThat(openStreams.list()).singleElement().satisfies(stream -> {
            assertThat(stream.client()).isEqualTo("Java-http-client/25.0.1");
            assertThat(stream.position().get()).isEqualTo(41L);
        });
        var commandCaptor = ArgumentCaptor.forClass(RouterActor.Command.class);
        verify(actorRegistry).tellRouter(commandCaptor.capture());
        var command = (RouterActor.RegisterSseStream) commandCaptor.getValue();
        command.onObserved().accept(57L);
        assertThat(openStreams.list().getFirst().position().get()).isEqualTo(57L);
        assertThat(openStreams.list().getFirst().id()).isEqualTo(command.streamId());
    }

    @Test
    void stream_leavesTheOpenListWhenItCompletes() throws Exception {
        lastPersistedIndexIs(100L);

        var result = mockMvc.perform(get("/api/events/stream").param("startIndex", "1"))
                .andExpect(request().asyncStarted())
                .andReturn();
        assertThat(openStreams.list()).hasSize(1);

        var commandCaptor = ArgumentCaptor.forClass(RouterActor.Command.class);
        verify(actorRegistry).tellRouter(commandCaptor.capture());
        ((RouterActor.RegisterSseStream) commandCaptor.getValue()).emitter().complete();
        // What the servlet container does once the emitter completes.
        result.getRequest().getAsyncContext().complete();

        assertThat(openStreams.list()).isEmpty();
    }

    @Test
    void clientOf_namesBrowsersAndKeepsTheFirstProductOtherwise() {
        var browser = new MockHttpServletRequest();
        browser.addHeader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/141.0");
        var curl = new MockHttpServletRequest();
        curl.addHeader("User-Agent", "curl/8.7.1");

        assertThat(EventStreamController.clientOf(browser)).isEqualTo("browser");
        assertThat(EventStreamController.clientOf(curl)).isEqualTo("curl/8.7.1");
        assertThat(EventStreamController.clientOf(new MockHttpServletRequest())).isEqualTo("unknown");
    }

    @Test
    void clientOf_prefersTheClientNameHeader() {
        var named = new MockHttpServletRequest();
        named.addHeader("User-Agent", "curl/8.7.1");
        named.addHeader("X-Client-Name", "  billing-reconciler\n ");
        var blank = new MockHttpServletRequest();
        blank.addHeader("User-Agent", "curl/8.7.1");
        blank.addHeader("X-Client-Name", "   ");
        var long_ = new MockHttpServletRequest();
        long_.addHeader("X-Client-Name", "x".repeat(80));

        assertThat(EventStreamController.clientOf(named)).isEqualTo("billing-reconciler");
        assertThat(EventStreamController.clientOf(blank)).isEqualTo("curl/8.7.1");
        assertThat(EventStreamController.clientOf(long_)).hasSize(60);
    }

    @Test
    void stream_allowsMissingFilters() throws Exception {
        lastPersistedIndexIs(100L);

        var mvcResult = mockMvc.perform(get("/api/events/stream")
                        .param("startIndex", "1"))
                .andExpect(request().asyncStarted())
                .andReturn();

        assertThat(mvcResult.getResponse().getStatus()).isEqualTo(200);

        var commandCaptor = ArgumentCaptor.forClass(RouterActor.Command.class);
        verify(actorRegistry).tellRouter(commandCaptor.capture());
        assertThat(commandCaptor.getValue()).isInstanceOfSatisfying(RouterActor.RegisterSseStream.class, command -> {
            assertThat(command.requestParameters().messageType()).isEmpty();
            assertThat(command.requestParameters().userIds()).isEmpty();
            assertThat(command.requestParameters().startIndex()).isEqualTo(1L);
        });
    }

    @Test
    void stream_acceptsOptionalEndIndex() throws Exception {
        lastPersistedIndexIs(100L);

        var mvcResult = mockMvc.perform(get("/api/events/stream")
                        .param("startIndex", "10")
                        .param("endIndex", "20"))
                .andExpect(request().asyncStarted())
                .andReturn();

        assertThat(mvcResult.getResponse().getStatus()).isEqualTo(200);

        var commandCaptor = ArgumentCaptor.forClass(RouterActor.Command.class);
        verify(actorRegistry).tellRouter(commandCaptor.capture());
        assertThat(commandCaptor.getValue()).isInstanceOfSatisfying(RouterActor.RegisterSseStream.class, command -> {
            assertThat(command.requestParameters().startIndex()).isEqualTo(10L);
            assertThat(command.requestParameters().endIndex()).isEqualTo(20L);
        });
    }

    @Test
    void stream_acceptsShardParameters() throws Exception {
        lastPersistedIndexIs(100L);

        var mvcResult = mockMvc.perform(get("/api/events/stream")
                        .param("startIndex", "10")
                        .param("shardCount", "16")
                        .param("shardIndex", "3"))
                .andExpect(request().asyncStarted())
                .andReturn();

        assertThat(mvcResult.getResponse().getStatus()).isEqualTo(200);

        var commandCaptor = ArgumentCaptor.forClass(RouterActor.Command.class);
        verify(actorRegistry).tellRouter(commandCaptor.capture());
        assertThat(commandCaptor.getValue()).isInstanceOfSatisfying(RouterActor.RegisterSseStream.class, command -> {
            assertThat(command.requestParameters().shardCount()).isEqualTo(16);
            assertThat(command.requestParameters().shardIndex()).isEqualTo(3);
        });
    }

    @Test
    void stream_returnsBadRequestWhenStartIndexMissing() {
        assertThatThrownBy(() -> controller.stream(
                null,
                null,
                null,
                null,
                null,
                null,
                new MockHttpServletRequest()))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("startIndex must be greater than 0");

        verifyNoInteractions(actorRegistry);
    }

    @Test
    void stream_returnsBadRequestWhenStartIndexIsNotPositive() {
        assertBadRequest(
                () -> controller.stream(null, null, 0L, null, null, null, new MockHttpServletRequest()),
                "startIndex must be greater than 0");

        verifyNoInteractions(actorRegistry);
    }

    @Test
    void stream_returnsBadRequestWhenEndIndexIsBeforeStartIndex() {
        assertBadRequest(
                () -> controller.stream(null, null, 11L, 10L, null, null, new MockHttpServletRequest()),
                "endIndex must be greater than or equal to startIndex");

        verify(actorRegistry, never()).tellRouter(any());
    }

    @Test
    void stream_acceptsStartIndexImmediatelyAfterLastPersistedIndex() throws Exception {
        lastPersistedIndexIs(10L);

        var mvcResult = mockMvc.perform(get("/api/events/stream")
                        .param("startIndex", "11"))
                .andExpect(request().asyncStarted())
                .andReturn();

        assertThat(mvcResult.getResponse().getStatus()).isEqualTo(200);

        var commandCaptor = ArgumentCaptor.forClass(RouterActor.Command.class);
        verify(actorRegistry).tellRouter(commandCaptor.capture());
        assertThat(commandCaptor.getValue()).isInstanceOfSatisfying(RouterActor.RegisterSseStream.class, command -> {
            assertThat(command.requestParameters().startIndex()).isEqualTo(11L);
        });
    }

    @Test
    void stream_acceptsStartIndexOneWhenNoMessagesHaveBeenPersisted() throws Exception {
        lastPersistedIndexIs(0L);

        var mvcResult = mockMvc.perform(get("/api/events/stream")
                        .param("startIndex", "1"))
                .andExpect(request().asyncStarted())
                .andReturn();

        assertThat(mvcResult.getResponse().getStatus()).isEqualTo(200);

        var commandCaptor = ArgumentCaptor.forClass(RouterActor.Command.class);
        verify(actorRegistry).tellRouter(commandCaptor.capture());
        assertThat(commandCaptor.getValue()).isInstanceOfSatisfying(RouterActor.RegisterSseStream.class, command -> {
            assertThat(command.requestParameters().startIndex()).isEqualTo(1L);
        });
    }

    @Test
    void stream_returnsBadRequestWhenStartIndexSkipsBeyondNextPersistedIndex() {
        lastPersistedIndexIs(10L);

        assertBadRequest(
                () -> controller.stream(null, null, 12L, null, null, null, new MockHttpServletRequest()),
                "startIndex must be less than or equal to last persisted index + 1");

        verify(actorRegistry, never()).tellRouter(any());
    }

    @Test
    void stream_returnsBadRequestWhenShardCountMissing() {
        assertBadRequest(
                () -> controller.stream(null, null, 1L, null, null, 0, new MockHttpServletRequest()),
                "shardCount and shardIndex must be provided together");

        verify(actorRegistry, never()).tellRouter(any());
    }

    @Test
    void stream_returnsBadRequestWhenShardIndexExceedsRange() {
        assertBadRequest(
                () -> controller.stream(null, null, 1L, null, 8, 8, new MockHttpServletRequest()),
                "shardIndex '8'");

        verify(actorRegistry, never()).tellRouter(any());
    }

    private void assertBadRequest(Runnable action, String message) {
        assertThatThrownBy(action::run)
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining(message);
    }

    private void lastPersistedIndexIs(long value) {
        org.mockito.Mockito.when(indexService.current()).thenReturn(new Index(value));
    }
}
