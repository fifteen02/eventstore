package com.fifteen02.eventstore.controller;

import com.fifteen02.eventstore.pekko.ActorRegistry;
import com.fifteen02.eventstore.pekko.actors.RouterActor;
import com.fifteen02.eventstore.service.IndexService;
import com.fifteen02.eventstore.service.OpenStreams;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Pull API: streams the log over Server-Sent Events. Requires the bearer token.
 *
 * <p>Each request gets its own streaming actor that replays stored messages from
 * {@code startIndex} and then continues with live messages. The server keeps no position for the
 * client; to resume, reconnect with {@code startIndex = last received id + 1}. Events:
 * {@code connected}, {@code message}, {@code heartbeat} and, with {@code endIndex},
 * {@code stream-ended}. See {@code docs/consuming.md}.
 */
@RestController
@RequestMapping("/api/events")
public class EventStreamController {

    private static final Logger log = LoggerFactory.getLogger(EventStreamController.class);

    private final ActorRegistry actorRegistry;
    private final IndexService indexService;
    private final OpenStreams openStreams;

    public EventStreamController(ActorRegistry actorRegistry, IndexService indexService, OpenStreams openStreams) {
        this.actorRegistry = actorRegistry;
        this.indexService = indexService;
        this.openStreams = openStreams;
    }

    /**
     * @param messageType optional, repeatable; case-insensitive message type filter
     * @param userIds     optional, repeatable; exact {@code userId} filter
     * @param startIndex  first index to deliver; 1 to last persisted index + 1
     * @param endIndex    optional last index; the stream completes once it is reached
     * @param shardCount  optional, with {@code shardIndex}; number of shards by murmur3 hash of {@code userId}
     * @param shardIndex  optional, with {@code shardCount}; shard to deliver, in {@code [0, shardCount)}
     */
    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(
            @RequestParam(name = "messageType", required = false) Set<String> messageType,
            @RequestParam(name = "userId", required = false) Set<String> userIds,
            @RequestParam(name = "startIndex") Long startIndex,
            @RequestParam(name = "endIndex", required = false) Long endIndex,
            @RequestParam(name = "shardCount", required = false) Integer shardCount,
            @RequestParam(name = "shardIndex", required = false) Integer shardIndex,
            HttpServletRequest request) {
        validateStartIndex(startIndex);
        validateEndIndex(startIndex, endIndex);
        validateShardParameters(shardCount, shardIndex);
        validateStartIndexIsPersisted(startIndex);
        // A stream registered before the router is up would be dropped and hang with no events, not
        // even heartbeats. Clients reconnect right after a restart, so say so and let them retry.
        if (!actorRegistry.isReady()) {
            throw startingUp();
        }

        var emitter = new SseEmitter(0L);
        var requestParameters = new StreamRequestParameters(
                normaliseRequestedMessageTypes(messageType),
                normaliseRequestedUserIds(userIds),
                startIndex,
                endIndex,
                shardCount,
                shardIndex);
        var streamId = UUID.randomUUID().toString();
        var open = openStreams.open(streamId, requestParameters, clientOf(request), request.getRemoteAddr());
        actorRegistry.tellRouter(new RouterActor.RegisterSseStream(
                streamId,
                emitter,
                requestParameters,
                open::observed));

        var unregister = (Runnable) () -> {
            openStreams.close(streamId);
            actorRegistry.tellRouter(new RouterActor.UnregisterSseStream(streamId));
        };

        emitter.onCompletion(() -> {
            unregister.run();
            log.debug("SSE stream completed for {} {}", streamId, requestParameters);
        });
        emitter.onTimeout(() -> {
            unregister.run();
            log.debug("SSE stream timed out for {} {}", streamId, requestParameters);
        });
        emitter.onError(error -> {
            unregister.run();
            log.debug("SSE stream error for {} {}", streamId, requestParameters, error);
        });

        return emitter;
    }

    /** 503 with {@code Retry-After: 1}, for the moment between the web server and the router starting. */
    private static ResponseStatusException startingUp() {
        return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "EventStore is starting; retry in a moment") {
            @Override
            public HttpHeaders getHeaders() {
                var headers = new HttpHeaders();
                headers.set(HttpHeaders.RETRY_AFTER, "1");
                return headers;
            }
        };
    }

    /** Optional request header that names the stream in the debug console. */
    static final String CLIENT_NAME_HEADER = "X-Client-Name";
    private static final int MAX_CLIENT_NAME = 60;

    /**
     * A short name for whoever opened the stream: the {@code X-Client-Name} header when it is sent,
     * otherwise "browser" or the first product in the User-Agent.
     */
    static String clientOf(HttpServletRequest request) {
        String named = request.getHeader(CLIENT_NAME_HEADER);
        if (named != null) {
            String clean = named.replaceAll("\\p{Cntrl}", "").trim();
            if (!clean.isEmpty()) {
                return clean.length() > MAX_CLIENT_NAME ? clean.substring(0, MAX_CLIENT_NAME) : clean;
            }
        }
        String userAgent = request.getHeader("User-Agent");
        if (userAgent == null || userAgent.isBlank()) {
            return "unknown";
        }
        if (userAgent.startsWith("Mozilla/")) {
            return "browser";
        }
        String first = userAgent.trim().split("\\s+")[0];
        return first.length() > 40 ? first.substring(0, 40) : first;
    }

    private Set<String> normaliseRequestedMessageTypes(Set<String> messageType) {
        if (messageType == null || messageType.isEmpty()) {
            return Set.of();
        }
        return messageType.stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }

    private Set<String> normaliseRequestedUserIds(Set<String> userIds) {
        if (userIds == null || userIds.isEmpty()) {
            return Set.of();
        }
        return userIds.stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }

    private void validateStartIndex(Long startIndex) {
        if (startIndex == null || startIndex < 1L) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Invalid startIndex '" + startIndex + "'. startIndex must be greater than 0");
        }
    }

    private void validateEndIndex(Long startIndex, Long endIndex) {
        if (endIndex == null) {
            return;
        }
        if (endIndex < startIndex) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Invalid endIndex '" + endIndex + "'. endIndex must be greater than or equal to startIndex");
        }
    }

    private void validateStartIndexIsPersisted(Long startIndex) {
        long lastPersistedIndex = indexService.current().value();
        long nextPersistableIndex = lastPersistedIndex + 1L;
        if (startIndex > nextPersistableIndex) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Invalid startIndex '" + startIndex
                            + "'. startIndex must be less than or equal to last persisted index + 1 '"
                            + nextPersistableIndex + "'");
        }
    }

    private void validateShardParameters(Integer shardCount, Integer shardIndex) {
        if (shardCount == null && shardIndex == null) {
            return;
        }
        if (shardCount == null || shardIndex == null) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Invalid shard parameters. shardCount and shardIndex must be provided together");
        }
        if (shardCount <= 0) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Invalid shardCount '" + shardCount + "'. shardCount must be greater than 0");
        }
        if (shardIndex < 0 || shardIndex >= shardCount) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Invalid shardIndex '" + shardIndex + "'. shardIndex must be in range [0.." + (shardCount - 1) + "]");
        }
    }
}
