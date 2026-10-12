package com.fifteen02.eventstore.controller;

import com.fifteen02.eventstore.config.InterfaceType;
import com.fifteen02.eventstore.config.InterfacesConfigProperties;
import com.fifteen02.eventstore.service.IndexService;
import com.fifteen02.eventstore.service.InterfaceStateService;
import com.fifteen02.eventstore.service.OpenStreams;
import com.fifteen02.eventstore.to.Index;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Set;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class DebugOverviewControllerTest {

    private MockMvc mockMvc;
    private final InterfacesConfigProperties config = new InterfacesConfigProperties();
    private final OpenStreams openStreams = new OpenStreams();

    @Mock
    private IndexService indexService;
    @Mock
    private InterfaceStateService interfaceStateService;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(
                new DebugOverviewController(indexService, interfaceStateService, config, openStreams, true)).build();
    }

    @Test
    void overview_reportsHeadAndHowFarEachInterfaceIsBehind() throws Exception {
        var ack = new InterfacesConfigProperties.Ack();
        ack.setBatch(5L);
        config.getInterfaces().add(definition("billing", InterfaceType.replayable, ack, Set.of("OrderPlaced", "InvoiceSent")));
        config.getInterfaces().add(definition("notifications", InterfaceType.realtime, null, null));
        when(indexService.current()).thenReturn(new Index(178L));
        when(interfaceStateService.getLastAck("billing")).thenReturn(175L);
        when(interfaceStateService.isRunning("billing")).thenReturn(true);
        when(interfaceStateService.isRunning("notifications")).thenReturn(true);

        mockMvc.perform(get("/api/debug/overview"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.headIndex").value(178))
                .andExpect(jsonPath("$.amqpEnabled").value(true))
                .andExpect(jsonPath("$.interfaces[0].name").value("billing"))
                .andExpect(jsonPath("$.interfaces[0].type").value("replayable"))
                .andExpect(jsonPath("$.interfaces[0].includeTypes[0]").value("InvoiceSent"))
                .andExpect(jsonPath("$.interfaces[0].position").value(175))
                .andExpect(jsonPath("$.interfaces[0].lag").value(3))
                .andExpect(jsonPath("$.interfaces[0].ackBatch").value(5))
                .andExpect(jsonPath("$.interfaces[1].type").value("realtime"))
                .andExpect(jsonPath("$.interfaces[1].position").doesNotExist())
                .andExpect(jsonPath("$.interfaces[1].lag").doesNotExist());
    }

    @Test
    void overview_listsOpenStreamsWithHowFarBehindTheyAre() throws Exception {
        when(indexService.current()).thenReturn(new Index(178L));
        var stream = openStreams.open("4f1c2a9e-0000-0000-0000-000000000000",
                new StreamRequestParameters(Set.of("OrderPlaced"), Set.of(), 100L, null, 2, 0), "curl/8.7.1", "10.0.0.7");
        stream.observed(170L);
        openStreams.open("closed-0-0000-0000-0000-000000000000",
                new StreamRequestParameters(Set.of(), Set.of(), 1L, null, null, null), "browser", "127.0.0.1");
        openStreams.close("closed-0-0000-0000-0000-000000000000");

        mockMvc.perform(get("/api/debug/overview"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.streams.length()").value(1))
                .andExpect(jsonPath("$.streams[0].id").value("4f1c2a9e"))
                .andExpect(jsonPath("$.streams[0].client").value("curl/8.7.1"))
                .andExpect(jsonPath("$.streams[0].remoteAddress").value("10.0.0.7"))
                .andExpect(jsonPath("$.streams[0].position").value(170))
                .andExpect(jsonPath("$.streams[0].lag").value(8))
                .andExpect(jsonPath("$.streams[0].startIndex").value(100))
                .andExpect(jsonPath("$.streams[0].messageTypes[0]").value("OrderPlaced"))
                .andExpect(jsonPath("$.streams[0].shardIndex").value(0))
                .andExpect(jsonPath("$.streams[0].shardCount").value(2));
    }

    @Test
    void overview_usesDefaultAckBatchWhenNoneConfigured() throws Exception {
        config.getInterfaces().add(definition("audit", InterfaceType.persister, null, null));
        when(indexService.current()).thenReturn(new Index(10L));
        when(interfaceStateService.getLastAck("audit")).thenReturn(12L);
        when(interfaceStateService.isRunning("audit")).thenReturn(false);

        mockMvc.perform(get("/api/debug/overview"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.interfaces[0].lag").value(0))
                .andExpect(jsonPath("$.interfaces[0].running").value(false))
                .andExpect(jsonPath("$.interfaces[0].ackBatch").value(1000));
    }

    private static InterfacesConfigProperties.InterfaceDefinition definition(String name, InterfaceType type,
                                                                            InterfacesConfigProperties.Ack ack,
                                                                            Set<String> includeTypes) {
        var def = new InterfacesConfigProperties.InterfaceDefinition();
        def.setName(name);
        def.setType(type);
        def.setAck(ack);
        def.setIncludeTypes(includeTypes);
        return def;
    }
}
