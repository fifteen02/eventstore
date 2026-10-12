package com.fifteen02.eventstore.controller;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.ClassPathResource;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class DevDebugControllerTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(DevDebugController.class);

    @Test
    void createsDebugControllerWhenDevProfileIsActive() {
        contextRunner
                .withPropertyValues("spring.profiles.active=dev")
                .run(context -> assertThat(context).hasSingleBean(DevDebugController.class));
    }

    @Test
    void createsDebugControllerWhenDefaultProfileIsDev() {
        contextRunner
                .withPropertyValues("spring.profiles.default=dev")
                .run(context -> assertThat(context).hasSingleBean(DevDebugController.class));
    }

    @Test
    void createsDebugControllerWhenDevProfileIsNotActive() {
        contextRunner
                .withPropertyValues("spring.profiles.active=live")
                .run(context -> assertThat(context).hasSingleBean(DevDebugController.class));
    }

    @Test
    void debugPageTakesTheTokenFromAnInputAndSendsItAsBearer() throws Exception {
        assertThat(debugPage())
                .contains("id=\"token\"")
                .contains("Authorization: `Bearer ${tokenInput.value.trim()}`")
                .doesNotContain("const AUTH_TOKEN");
    }

    @Test
    void debugPageIsSelfContained() throws Exception {
        assertThat(debugPage())
                .doesNotContain("<script src=")
                .doesNotContain("<link rel=\"stylesheet\"")
                .doesNotContain("@import");
    }

    @Test
    void debugPageUsesTheEventAndDebugApis() throws Exception {
        assertThat(debugPage())
                .contains("/api/debug/overview")
                .contains("/api/debug/messages")
                .contains("/api/events/stream")
                .contains("/api/events/message")
                .doesNotContain("DELETE /api/debug/messages")
                .doesNotContain("POST /api/debug/messages")
                .doesNotContain("PUT /api/debug/messages");
    }

    @Test
    void debugPageShowsHowFarTheStreamIsBehind() throws Exception {
        assertThat(debugPage())
                .contains("behind(data.lastPersistedIndex)");
    }

    private static String debugPage() throws Exception {
        return new ClassPathResource("debug/eventstore-debug.html").getContentAsString(StandardCharsets.UTF_8);
    }

}
