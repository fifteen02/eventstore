package com.fifteen02.eventstore.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * Caps the body of the HTTP input endpoints ({@code eventstore.input.max-body-bytes}, 1 MiB by default).
 * Spring Boot has no property for this: Tomcat's limits cover form posts only, not a JSON {@code PUT}.
 * A chunked body has no {@code Content-Length}, so it is read up to the limit before the controller runs.
 */
@Component
public class InputBodySizeLimitFilter extends OncePerRequestFilter {

    private static final Set<String> LIMITED_PATHS = Set.of("/api/events/message", "/api/events/aggregate-message");

    private final long maxBodyBytes;

    public InputBodySizeLimitFilter(@Value("${eventstore.input.max-body-bytes:1048576}") long maxBodyBytes) {
        if (maxBodyBytes <= 0 || maxBodyBytes > Integer.MAX_VALUE - 8) {
            throw new IllegalArgumentException("eventstore.input.max-body-bytes must be between 1 and "
                    + (Integer.MAX_VALUE - 8) + ", was " + maxBodyBytes);
        }
        this.maxBodyBytes = maxBodyBytes;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !LIMITED_PATHS.contains(pathWithinApplication(request));
    }

    // The servlet path is decoded and free of ;params, so "/api/events/message;x=y" cannot slip past.
    private String pathWithinApplication(HttpServletRequest request) {
        var servletPath = request.getServletPath();
        if (StringUtils.hasText(servletPath)) {
            return servletPath;
        }
        var requestUri = request.getRequestURI();
        var contextPath = request.getContextPath();
        if (StringUtils.hasText(contextPath) && requestUri.startsWith(contextPath)) {
            return requestUri.substring(contextPath.length());
        }
        return requestUri;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        long declared = request.getContentLengthLong();
        if (declared > maxBodyBytes) {
            tooLarge(response);
            return;
        }
        if (declared >= 0) {
            filterChain.doFilter(request, response);
            return;
        }

        byte[] body = request.getInputStream().readNBytes((int) maxBodyBytes + 1);
        if (body.length > maxBodyBytes) {
            tooLarge(response);
            return;
        }
        filterChain.doFilter(new CachedBodyRequest(request, body), response);
    }

    private void tooLarge(HttpServletResponse response) throws IOException {
        response.sendError(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE,
                "Request body is larger than " + maxBodyBytes + " bytes");
    }

    private static final class CachedBodyRequest extends HttpServletRequestWrapper {
        private final byte[] body;

        private CachedBodyRequest(HttpServletRequest request, byte[] body) {
            super(request);
            this.body = body;
        }

        @Override
        public ServletInputStream getInputStream() {
            var in = new ByteArrayInputStream(body);
            return new ServletInputStream() {
                @Override
                public int read() {
                    return in.read();
                }

                @Override
                public int read(byte[] buffer, int offset, int length) {
                    return in.read(buffer, offset, length);
                }

                @Override
                public boolean isFinished() {
                    return in.available() == 0;
                }

                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setReadListener(ReadListener readListener) {
                    throw new UnsupportedOperationException("Async reads are not supported");
                }
            };
        }

        @Override
        public BufferedReader getReader() {
            Charset charset = getCharacterEncoding() == null
                    ? StandardCharsets.UTF_8
                    : Charset.forName(getCharacterEncoding());
            return new BufferedReader(new InputStreamReader(getInputStream(), charset));
        }

        @Override
        public int getContentLength() {
            return body.length;
        }

        @Override
        public long getContentLengthLong() {
            return body.length;
        }
    }
}
