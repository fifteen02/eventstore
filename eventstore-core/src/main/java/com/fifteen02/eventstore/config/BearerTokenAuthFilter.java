package com.fifteen02.eventstore.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Set;

@Component
public class BearerTokenAuthFilter extends OncePerRequestFilter {

    private static final String AUTHORIZATION = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";
    private static final Set<String> PROTECTED_PATHS = Set.of(
            "/api/events/message",
            "/api/events/aggregate-message",
            "/api/events/stream",
            "/api/debug/messages",
            "/api/debug/overview");

    private final String bearerToken;

    public BearerTokenAuthFilter(@Value("${eventstore.auth.bearer-token}") String bearerToken) {
        if (!StringUtils.hasText(bearerToken)) {
            throw new IllegalArgumentException("eventstore.auth.bearer-token must be configured");
        }
        this.bearerToken = bearerToken;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        if (!isProtected(request) || hasValidBearerToken(request)) {
            filterChain.doFilter(request, response);
            return;
        }

        response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Missing or invalid bearer token");
    }

    private boolean isProtected(HttpServletRequest request) {
        return !"OPTIONS".equals(request.getMethod()) && PROTECTED_PATHS.contains(pathWithinApplication(request));
    }

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

    private boolean hasValidBearerToken(HttpServletRequest request) {
        var authorization = request.getHeader(AUTHORIZATION);
        if (authorization == null || !authorization.startsWith(BEARER_PREFIX)) {
            return false;
        }
        return secureEquals(authorization.substring(BEARER_PREFIX.length()), bearerToken);
    }

    private boolean secureEquals(String actual, String expected) {
        return MessageDigest.isEqual(
                actual.getBytes(StandardCharsets.UTF_8),
                expected.getBytes(StandardCharsets.UTF_8));
    }
}
