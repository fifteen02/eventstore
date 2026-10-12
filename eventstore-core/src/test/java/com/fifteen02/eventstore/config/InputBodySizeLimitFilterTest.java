package com.fifteen02.eventstore.config;

import jakarta.servlet.ServletRequest;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class InputBodySizeLimitFilterTest {

    private final InputBodySizeLimitFilter filter = new InputBodySizeLimitFilter(16);

    @Test
    void rejectsADeclaredContentLengthOverTheLimit() throws Exception {
        var request = put("/api/events/message", "{\"content\":\"0123456789\"}");
        var response = new MockHttpServletResponse();
        var chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertThat(response.getStatus()).isEqualTo(413);
        assertThat(chain.getRequest()).isNull();
    }

    @Test
    void rejectsAChunkedBodyOverTheLimit() throws Exception {
        var request = chunked("/api/events/aggregate-message", "{\"content\":\"0123456789\"}");
        var response = new MockHttpServletResponse();
        var chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertThat(response.getStatus()).isEqualTo(413);
        assertThat(chain.getRequest()).isNull();
    }

    @Test
    void passesAChunkedBodyWithinTheLimitThroughUnchanged() throws Exception {
        var request = chunked("/api/events/message", "{\"a\":1}");
        var response = new MockHttpServletResponse();
        var chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertThat(response.getStatus()).isEqualTo(200);
        ServletRequest passed = chain.getRequest();
        assertThat(new String(passed.getInputStream().readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("{\"a\":1}");
    }

    @Test
    void matchesTheServletPathSoPathParametersCannotBypassTheLimit() throws Exception {
        var request = put("/api/events/message;x=y", "{\"content\":\"0123456789\"}");
        request.setServletPath("/api/events/message");
        var response = new MockHttpServletResponse();
        var chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertThat(response.getStatus()).isEqualTo(413);
        assertThat(chain.getRequest()).isNull();
    }

    @Test
    void ignoresOtherPaths() throws Exception {
        var request = put("/api/events/stream", "{\"content\":\"0123456789\"}");
        var response = new MockHttpServletResponse();
        var chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertThat(chain.getRequest()).isSameAs(request);
    }

    private static MockHttpServletRequest put(String path, String body) {
        var request = new MockHttpServletRequest("PUT", path);
        request.setContent(body.getBytes(StandardCharsets.UTF_8));
        return request;
    }

    /** No Content-Length, as with {@code Transfer-Encoding: chunked}. */
    private static MockHttpServletRequest chunked(String path, String body) {
        return new MockHttpServletRequest("PUT", path) {
            @Override
            public long getContentLengthLong() {
                return -1;
            }

            @Override
            public int getContentLength() {
                return -1;
            }

            @Override
            public jakarta.servlet.ServletInputStream getInputStream() {
                InputStream in = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
                return new org.springframework.mock.web.DelegatingServletInputStream(in);
            }
        };
    }
}
