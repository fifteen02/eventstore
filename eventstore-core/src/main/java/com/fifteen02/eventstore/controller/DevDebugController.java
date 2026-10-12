package com.fifteen02.eventstore.controller;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class DevDebugController {

    private static final Resource DEBUG_PAGE = new ClassPathResource("debug/eventstore-debug.html");

    @GetMapping("/debug/events")
    public ResponseEntity<Resource> eventsDebugPage() {
        return ResponseEntity.ok()
                .contentType(MediaType.TEXT_HTML)
                .body(DEBUG_PAGE);
    }
}
