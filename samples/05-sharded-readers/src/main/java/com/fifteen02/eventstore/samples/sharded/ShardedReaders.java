package com.fifteen02.eventstore.samples.sharded;

import tools.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Splits the log across N pull readers with {@code shardCount} / {@code shardIndex}.
 *
 * <p>Each reader opens its own SSE stream over the same index range, but asks only for its shard.
 * EventStore assigns a message to a shard by hashing its {@code userId}, so:
 * <ul>
 *   <li>every message is delivered to exactly one reader;</li>
 *   <li>all messages of one user go to the same reader, in order;</li>
 *   <li>messages without a {@code userId} all go to the same shard.</li>
 * </ul>
 *
 * <p>The readers replay the range {@code 1..last stored index} (a bounded stream ends by itself) and the
 * program prints which users each shard received and checks that nothing was lost or duplicated.
 * Set {@code SHARDS} to change the number of readers (default 3).
 */
public final class ShardedReaders {

    private static final String EVENTSTORE_URL = env("EVENTSTORE_URL", "http://localhost:8083");
    private static final String EVENTSTORE_TOKEN = env("EVENTSTORE_TOKEN", "dev-eventstore-token");
    private static final int SHARDS = Integer.parseInt(env("SHARDS", "3"));

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    /** One delivered message: its index and user. */
    private record Delivered(long id, String userId) {
    }

    public static void main(String[] args) throws Exception {
        long endIndex = lastStoredIndex();
        System.out.printf("reading indices 1..%d with %d shard readers%n%n", endIndex, SHARDS);

        Map<Integer, List<Delivered>> byShard = new ConcurrentHashMap<>();
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> readers = new ArrayList<>();
            for (int shard = 0; shard < SHARDS; shard++) {
                int shardIndex = shard;
                readers.add(pool.submit(() -> {
                    byShard.put(shardIndex, read(shardIndex, endIndex));
                    return null;
                }));
            }
            for (Future<?> reader : readers) {
                reader.get();
            }
        }

        var seen = new TreeSet<Long>();
        int duplicates = 0;
        for (int shard = 0; shard < SHARDS; shard++) {
            var delivered = byShard.get(shard);
            Map<String, Integer> perUser = new TreeMap<>();
            for (var d : delivered) {
                perUser.merge(d.userId() == null ? "(no user)" : d.userId(), 1, Integer::sum);
                if (!seen.add(d.id())) {
                    duplicates++;
                }
            }
            System.out.printf("shard %d: %3d messages  %s%n", shard, delivered.size(), perUser);
        }
        System.out.printf("%ntotal %d distinct of %d stored, %d delivered twice%n", seen.size(), endIndex, duplicates);
    }

    /** One shard reader: a bounded SSE stream that only carries this shard's messages. */
    private static List<Delivered> read(int shardIndex, long endIndex) throws Exception {
        var uri = URI.create(EVENTSTORE_URL + "/api/events/stream?startIndex=1&endIndex=" + endIndex
                + "&shardCount=" + SHARDS + "&shardIndex=" + shardIndex);
        var response = HTTP.send(authorized(uri), HttpResponse.BodyHandlers.ofInputStream());
        var delivered = new ArrayList<Delivered>();
        try (var reader = new BufferedReader(new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
            String event = null;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("event:")) {
                    event = line.substring(6).trim();
                } else if (line.startsWith("data:") && "message".equals(event)) {
                    var message = JSON.readTree(line.substring(5)).path("message");
                    delivered.add(new Delivered(message.path("id").asLong(),
                            message.path("userId").isNull() ? null : message.path("userId").asString()));
                }
            }
        }
        return delivered;
    }

    /** The newest index, from the {@code connected} event of a stream that asks for nothing yet. */
    private static long lastStoredIndex() throws Exception {
        var response = HTTP.send(authorized(URI.create(EVENTSTORE_URL + "/api/events/stream?startIndex=1&endIndex=1")),
                HttpResponse.BodyHandlers.ofLines());
        return response.body()
                .filter(line -> line.startsWith("data:"))
                .findFirst()
                .map(line -> JSON.readTree(line.substring(5)).path("lastPersistedIndex").asLong())
                .orElseThrow(() -> new IllegalStateException("no connected event"));
    }

    private static HttpRequest authorized(URI uri) {
        return HttpRequest.newBuilder(uri)
                .header("Authorization", "Bearer " + EVENTSTORE_TOKEN)
                .header("Accept", "text/event-stream")
                .build();
    }

    private static String env(String name, String defaultValue) {
        var value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
