package com.fifteen02.eventstore.samples.pull;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * Pull consumer: reads the EventStore log over Server-Sent Events and survives restarts on both sides.
 *
 * <p>The client owns its position. It keeps the last processed index in a checkpoint file and,
 * on every (re)connect, asks for {@code startIndex = checkpoint + 1}. EventStore replays what is
 * stored from that index and then keeps the connection open for live messages.
 *
 * <p>Environment variables:
 * <ul>
 *   <li>{@code STREAM_TYPES}: comma-separated message types to receive (default: all);</li>
 *   <li>{@code END_INDEX}: stop once this index is reached (a bounded replay);</li>
 *   <li>{@code CHECKPOINT_FILE}: where progress is kept (default: {@code target/stream.checkpoint} of this sample);</li>
 *   <li>{@code EVENTSTORE_URL}, {@code EVENTSTORE_TOKEN}: where to connect.</li>
 * </ul>
 */
public final class StreamConsumer {

    private static final String EVENTSTORE_URL = env("EVENTSTORE_URL", "http://localhost:8083");
    private static final String EVENTSTORE_TOKEN = env("EVENTSTORE_TOKEN", "dev-eventstore-token");
    private static final String STREAM_TYPES = env("STREAM_TYPES", "");
    private static final String END_INDEX = env("END_INDEX", "");

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private final Path checkpoint;

    private StreamConsumer(Path checkpoint) {
        this.checkpoint = checkpoint;
    }

    public static void main(String[] args) throws Exception {
        new StreamConsumer(checkpointFile()).run();
    }

    private void run() throws Exception {
        System.out.println("stream: checkpoint file " + checkpoint);
        while (true) {
            long startIndex = readCheckpoint() + 1;
            if (!END_INDEX.isBlank() && startIndex > Long.parseLong(END_INDEX)) {
                System.out.println("stream: checkpoint is already past END_INDEX, nothing to do");
                return;
            }
            try {
                if (consume(startIndex)) {
                    return; // the server ended the stream at END_INDEX
                }
                System.out.println("stream: connection closed, reconnecting");
            } catch (Exception e) {
                System.out.println("stream: " + e.getMessage() + ", reconnecting in 2s");
            }
            Thread.sleep(2_000);
        }
    }

    /** Reads one connection until it closes. Returns true when the server sent {@code stream-ended}. */
    private boolean consume(long startIndex) throws Exception {
        var request = HttpRequest.newBuilder(streamUri(startIndex))
                .header("Authorization", "Bearer " + EVENTSTORE_TOKEN)
                .header("Accept", "text/event-stream")
                .build();
        var response = HTTP.send(request, HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() != 200) {
            response.body().close();
            throw new IllegalStateException("HTTP " + response.statusCode() + " from " + request.uri());
        }

        // Minimal SSE parser: "event:" and "data:" lines, a blank line ends one event.
        try (var reader = new BufferedReader(new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
            String event = null;
            var data = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    if (event != null && handle(event, JSON.readTree(data.toString()))) {
                        return true;
                    }
                    event = null;
                    data.setLength(0);
                } else if (line.startsWith("event:")) {
                    event = line.substring(6).trim();
                } else if (line.startsWith("data:")) {
                    data.append(line.substring(5));
                }
            }
        }
        return false;
    }

    private boolean handle(String event, JsonNode data) throws Exception {
        long lastPersisted = data.path("lastPersistedIndex").asLong();
        switch (event) {
            case "connected" -> System.out.printf("stream: connected from %d, server is at %d%n",
                    data.path("filters").path("startIndex").asLong(), lastPersisted);
            case "message" -> {
                JsonNode message = data.path("message");
                long id = message.path("id").asLong();
                process(message, lastPersisted - id);
                // Checkpoint after processing: a crash in between re-delivers the message, never skips it.
                writeCheckpoint(id);
            }
            case "heartbeat" -> { /* keep-alive; lastPersistedIndex tells us how far behind we are */ }
            case "stream-ended" -> {
                System.out.printf("stream: ended at %d (%s)%n", data.path("lastSentIndex").asLong(), data.path("reason").asString());
                writeCheckpoint(data.path("lastSentIndex").asLong());
                return true;
            }
            default -> System.out.println("stream: ignoring event " + event);
        }
        return false;
    }

    /** Your business logic goes here. Keep it idempotent: after a crash the last message may arrive again. */
    private void process(JsonNode message, long behind) {
        System.out.printf("stream: #%d %-14s user=%-10s %s%s%n",
                message.path("id").asLong(),
                message.path("messageType").asString(),
                message.path("userId").asString(),
                message.path("body"),
                behind > 0 ? "   (" + behind + " behind)" : "");
    }

    private static URI streamUri(long startIndex) {
        var query = new StringBuilder("startIndex=").append(startIndex);
        for (String type : STREAM_TYPES.split(",")) {
            if (!type.isBlank()) {
                query.append("&messageType=").append(URLEncoder.encode(type.trim(), StandardCharsets.UTF_8));
            }
        }
        if (!END_INDEX.isBlank()) {
            query.append("&endIndex=").append(END_INDEX);
        }
        return URI.create(EVENTSTORE_URL + "/api/events/stream?" + query);
    }

    private long readCheckpoint() throws Exception {
        return Files.exists(checkpoint) ? Long.parseLong(Files.readString(checkpoint).trim()) : 0L;
    }

    private void writeCheckpoint(long index) throws Exception {
        Files.writeString(checkpoint, Long.toString(index));
    }

    /** Defaults to target/stream.checkpoint of this sample, wherever Maven was started from. */
    private static Path checkpointFile() throws Exception {
        var configured = env("CHECKPOINT_FILE", "");
        if (!configured.isBlank()) {
            return Path.of(configured).toAbsolutePath();
        }
        var classes = Path.of(StreamConsumer.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        return classes.getParent().resolve("stream.checkpoint");
    }

    private static String env(String name, String defaultValue) {
        var value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
