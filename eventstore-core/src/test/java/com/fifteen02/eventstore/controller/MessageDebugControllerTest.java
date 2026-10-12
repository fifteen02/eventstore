package com.fifteen02.eventstore.controller;

import com.fifteen02.eventstore.entity.Message;
import com.fifteen02.eventstore.repo.MessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class MessageDebugControllerTest {

    private MockMvc mockMvc;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private MessageRepository messageRepository;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(new MessageDebugController(messageRepository))
                .build();
    }

    @Test
    void getMessages_returnsTabulatorPageOfMessages() throws Exception {
        var message = message(42L, "ProfileUpdatedEvent", "{\"ok\":true}");
        var pageable = PageRequest.of(0, 25, Sort.by(Sort.Direction.DESC, "id"));
        when(messageRepository.findAll(pageable))
                .thenReturn(new PageImpl<>(List.of(message), pageable, 51));

        mockMvc.perform(get("/api/debug/messages"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.last_page").value(3))
                .andExpect(jsonPath("$.total_count").value(51))
                .andExpect(jsonPath("$.data[0].id").value(42))
                .andExpect(jsonPath("$.data[0].messageType").value("ProfileUpdatedEvent"))
                .andExpect(jsonPath("$.data[0].body.ok").value(true));
    }

    @Test
    void getMessages_usesRequestedPageSizeAndSort() throws Exception {
        when(messageRepository.findAll(PageRequest.of(1, 10, Sort.by(Sort.Direction.ASC, "timestamp"))))
                .thenReturn(new PageImpl<>(List.of(), PageRequest.of(1, 10), 0));

        mockMvc.perform(get("/api/debug/messages")
                        .param("page", "2")
                        .param("size", "10")
                        .param("sort", "timestamp")
                        .param("dir", "asc"))
                .andExpect(status().isOk());

        var pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(messageRepository).findAll(pageableCaptor.capture());
        assertThat(pageableCaptor.getValue().getPageNumber()).isEqualTo(1);
        assertThat(pageableCaptor.getValue().getPageSize()).isEqualTo(10);
        assertThat(pageableCaptor.getValue().getSort())
                .isEqualTo(Sort.by(Sort.Direction.ASC, "timestamp"));
    }

    private Message message(long id, String type, String content) throws Exception {
        var message = new Message();
        message.setId(id);
        message.setType(type);
        message.setContent(json(content));
        message.setTimestamp(Instant.parse("2026-06-03T08:15:00Z"));
        return message;
    }

    private JsonNode json(String value) throws Exception {
        return objectMapper.readTree(value);
    }
}
