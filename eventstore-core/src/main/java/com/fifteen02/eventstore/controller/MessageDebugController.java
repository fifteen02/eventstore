package com.fifteen02.eventstore.controller;

import com.fifteen02.eventstore.repo.MessageRepository;
import com.fifteen02.eventstore.to.MessageTo;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/debug/messages")
public class MessageDebugController {

    private static final int DEFAULT_PAGE_SIZE = 25;
    private static final int MAX_PAGE_SIZE = 100;

    private final MessageRepository messageRepository;

    public MessageDebugController(MessageRepository messageRepository) {
        this.messageRepository = messageRepository;
    }

    @GetMapping
    public MessagesPage messages(@RequestParam(name = "page", defaultValue = "1") int page,
                                 @RequestParam(name = "size", defaultValue = "25") int size,
                                 @RequestParam(name = "sort", defaultValue = "id") String sort,
                                 @RequestParam(name = "dir", defaultValue = "desc") String dir) {
        var pageRequest = PageRequest.of(
                zeroBasedPage(page),
                pageSize(size),
                Sort.by(direction(dir), sortProperty(sort)));
        var messages = messageRepository.findAll(pageRequest);
        var data = messages.stream()
                .map(MessageTo::from)
                .toList();

        return new MessagesPage(data, messages.getTotalPages(), messages.getTotalElements());
    }

    private int zeroBasedPage(int page) {
        return Math.max(page, 1) - 1;
    }

    private int pageSize(int size) {
        if (size < 1) {
            return DEFAULT_PAGE_SIZE;
        }
        return Math.min(size, MAX_PAGE_SIZE);
    }

    private Sort.Direction direction(String dir) {
        return "asc".equalsIgnoreCase(dir) ? Sort.Direction.ASC : Sort.Direction.DESC;
    }

    private String sortProperty(String sort) {
        return switch (sort) {
            case "timestamp" -> "timestamp";
            case "messageType" -> "type";
            case "userId" -> "userId";
            case "correlationId" -> "correlationId";
            case "aggregateUuid" -> "aggregateUuid";
            case "aggregateIndex" -> "aggregateIndex";
            case "contentHash" -> "contentHash";
            default -> "id";
        };
    }

    public record MessagesPage(List<MessageTo> data, int last_page, long total_count) {
    }
}
