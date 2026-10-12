package com.fifteen02.eventstore.config;

import jakarta.servlet.ServletException;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BearerTokenAuthFilterTest {

    @Test
    void rejectsProtectedApiRequestWithoutBearerToken() throws ServletException, IOException {
        var filter = new BearerTokenAuthFilter("dev-eventstore-token");
        var request = new MockHttpServletRequest("PUT", "/api/events/message");
        var response = new MockHttpServletResponse();
        var chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertThat(response.getStatus()).isEqualTo(401);
    }

    @Test
    void rejectsDebugMessagesRequestWithoutBearerToken() throws ServletException, IOException {
        var filter = new BearerTokenAuthFilter("dev-eventstore-token");
        var request = new MockHttpServletRequest("GET", "/api/debug/messages");
        var response = new MockHttpServletResponse();
        var chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertThat(response.getStatus()).isEqualTo(401);
    }

    @Test
    void rejectsDebugOverviewRequestWithoutBearerToken() throws ServletException, IOException {
        var filter = new BearerTokenAuthFilter("dev-eventstore-token");
        var request = new MockHttpServletRequest("GET", "/api/debug/overview");
        var response = new MockHttpServletResponse();
        var chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertThat(response.getStatus()).isEqualTo(401);
        assertThat(chain.getRequest()).isNull();
    }

    @Test
    void rejectsProtectedApiRequestWithoutBearerTokenWhenApplicationHasContextPath() throws ServletException, IOException {
        var filter = new BearerTokenAuthFilter("dev-eventstore-token");
        var request = new MockHttpServletRequest("PUT", "/eventstore/api/events/message");
        request.setContextPath("/eventstore");
        request.setServletPath("/api/events/message");
        var response = new MockHttpServletResponse();
        var chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertThat(response.getStatus()).isEqualTo(401);
    }

    @Test
    void rejectsBlankConfiguredBearerToken() {
        assertThatThrownBy(() -> new BearerTokenAuthFilter(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("eventstore.auth.bearer-token must be configured");
    }

    @Test
    void rejectsProtectedApiRequestWithWrongBearerToken() throws ServletException, IOException {
        var filter = new BearerTokenAuthFilter("dev-eventstore-token");
        var request = new MockHttpServletRequest("GET", "/api/events/stream");
        request.addHeader("Authorization", "Bearer wrong-token");
        var response = new MockHttpServletResponse();
        var chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertThat(response.getStatus()).isEqualTo(401);
    }

    @Test
    void allowsProtectedApiRequestWithConfiguredBearerToken() throws ServletException, IOException {
        var filter = new BearerTokenAuthFilter("dev-eventstore-token");
        var request = new MockHttpServletRequest("PUT", "/api/events/aggregate-message");
        request.addHeader("Authorization", "Bearer dev-eventstore-token");
        var response = new MockHttpServletResponse();
        var chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertThat(response.getStatus()).isEqualTo(200);
    }

    @Test
    void leavesUnprotectedRequestAlone() throws ServletException, IOException {
        var filter = new BearerTokenAuthFilter("dev-eventstore-token");
        var request = new MockHttpServletRequest("GET", "/debug/events");
        var response = new MockHttpServletResponse();
        var chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertThat(response.getStatus()).isEqualTo(200);
    }
}
