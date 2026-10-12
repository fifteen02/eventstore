package com.fifteen02.eventstore.samples.projection;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A projection service: it folds the order events from EventStore into "orders per customer",
 * snapshots its state, and serves that state to its own subscribers.
 *
 * <ul>
 *   <li><b>Input</b>: an ordinary pull stream from EventStore, resumed from the snapshot's index + 1.</li>
 *   <li><b>Snapshots</b>: (index, state) written together every {@code SNAPSHOT_EVERY} events, so a
 *       restart folds only what happened since the last snapshot.</li>
 *   <li><b>Side effects</b>: a "new VIP" alert fires when a customer's spend crosses
 *       {@code VIP_THRESHOLD}, but only once the projection has caught up with the log; alerts
 *       during catch-up are about the past and are suppressed.</li>
 *   <li><b>Output</b>: {@code GET /state} returns the current state; {@code GET /stream} sends a
 *       {@code snapshot} event on connect and an {@code update} event per change. New subscribers get
 *       the current state immediately, never a replay of history.</li>
 * </ul>
 */
public final class ProjectionService {

    private static final String EVENTSTORE_URL = env("EVENTSTORE_URL", "http://localhost:8083");
    private static final String EVENTSTORE_TOKEN = env("EVENTSTORE_TOKEN", "dev-eventstore-token");
    private static final int PORT = Integer.parseInt(env("PORT", "8093"));
    private static final int SNAPSHOT_EVERY = Integer.parseInt(env("SNAPSHOT_EVERY", "20"));
    private static final long VIP_THRESHOLD = Long.parseLong(env("VIP_THRESHOLD", "100"));

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private final Path snapshotFile;
    private final OrderProjection projection;
    private final List<OutputStream> subscribers = new CopyOnWriteArrayList<>();
    private volatile boolean caughtUp;
    private long catchUpTarget;
    private int sinceSnapshot;
    private int suppressedAlerts;

    private ProjectionService(Path snapshotFile) throws Exception {
        this.snapshotFile = snapshotFile;
        this.projection = OrderProjection.load(snapshotFile);
    }

    public static void main(String[] args) throws Exception {
        var classes = Path.of(ProjectionService.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        var configured = env("SNAPSHOT_FILE", "");
        var service = new ProjectionService(configured.isBlank()
                ? classes.getParent().resolve("projection.snapshot.json")
                : Path.of(configured).toAbsolutePath());
        service.serve();
        service.consume();
    }

    // ---- input: fold the log ------------------------------------------------------------------

    private void consume() throws Exception {
        System.out.printf("projection: loaded snapshot at index %d from %s%n", projection.index(), snapshotFile);
        while (true) {
            try {
                readStream(projection.index() + 1);
                System.out.println("projection: connection closed, reconnecting");
            } catch (Exception e) {
                System.out.println("projection: " + e.getMessage() + ", reconnecting in 2s");
            }
            caughtUp = false;
            Thread.sleep(2_000);
        }
    }

    private void readStream(long startIndex) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(EVENTSTORE_URL + "/api/events/stream?startIndex=" + startIndex))
                .header("Authorization", "Bearer " + EVENTSTORE_TOKEN)
                .header("Accept", "text/event-stream")
                .build();
        var response = HTTP.send(request, HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() != 200) {
            response.body().close();
            throw new IllegalStateException("HTTP " + response.statusCode());
        }
        try (var reader = new BufferedReader(new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
            String event = null;
            var data = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    if (event != null) {
                        handle(event, JSON.readTree(data.toString()));
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
    }

    private void handle(String event, JsonNode data) throws Exception {
        long lastPersisted = data.path("lastPersistedIndex").asLong();
        switch (event) {
            case "connected" -> {
                catchUpTarget = lastPersisted;
                System.out.printf("projection: connected from %d, log is at %d%n", projection.index() + 1, lastPersisted);
                markCaughtUpIfReached();
            }
            case "message" -> fold(data.path("message"));
            case "heartbeat" -> markCaughtUpIfReached();
            default -> { }
        }
    }

    private void fold(JsonNode message) throws Exception {
        long id = message.path("id").asLong();
        String customer;
        OrderProjection.Stats before;
        OrderProjection.Stats after;
        synchronized (projection) {
            String user = message.path("userId").isNull() ? null : message.path("userId").asString();
            before = projection.stats(user == null ? "" : user);
            customer = projection.apply(id, message.path("messageType").asString(), user, message.path("body"));
            after = customer == null ? before : projection.stats(customer);
        }
        if (customer != null) {
            maybeAlert(customer, before, after);
            publish("update", updateJson(customer, after));
        }
        if (++sinceSnapshot >= SNAPSHOT_EVERY) {
            snapshot();
        }
        markCaughtUpIfReached();
    }

    /** Side effects only once caught up: during catch-up we are replaying the past. */
    private void maybeAlert(String customer, OrderProjection.Stats before, OrderProjection.Stats after) {
        if (before.spent() >= VIP_THRESHOLD || after.spent() < VIP_THRESHOLD) {
            return;
        }
        if (caughtUp) {
            System.out.printf("projection: ALERT %s is now a VIP (spent %d)%n", customer, after.spent());
        } else {
            suppressedAlerts++;
        }
    }

    private void markCaughtUpIfReached() throws Exception {
        if (!caughtUp && projection.index() >= catchUpTarget) {
            caughtUp = true;
            System.out.printf("projection: caught up at index %d (%d alerts suppressed during catch-up)%n",
                    projection.index(), suppressedAlerts);
            suppressedAlerts = 0;
            snapshot();
        }
    }

    private void snapshot() throws Exception {
        synchronized (projection) {
            projection.save(snapshotFile);
            System.out.printf("projection: snapshot at index %d%n", projection.index());
        }
        sinceSnapshot = 0;
    }

    // ---- output: serve the state ---------------------------------------------------------------

    private void serve() throws IOException {
        var server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/state", exchange -> {
            byte[] body = JSON.writeValueAsBytes(stateJson());
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (var out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.createContext("/stream", this::subscribe);
        server.start();
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                () -> publish("heartbeat", JSON.createObjectNode().put("index", projection.index()).put("caughtUp", caughtUp)),
                5, 5, TimeUnit.SECONDS);
        System.out.printf("projection: serving http://localhost:%d/state and /stream%n", PORT);
    }

    /** Connectable stream: the current state first, then every change. No history, no position. */
    private void subscribe(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
        exchange.sendResponseHeaders(200, 0);
        OutputStream out = exchange.getResponseBody();
        synchronized (projection) {
            write(out, "snapshot", stateJson());
            subscribers.add(out);
        }
    }

    private void publish(String event, JsonNode data) {
        for (OutputStream out : subscribers) {
            try {
                write(out, event, data);
            } catch (IOException e) {
                subscribers.remove(out);
            }
        }
    }

    private static void write(OutputStream out, String event, JsonNode data) throws IOException {
        synchronized (out) {
            out.write(("event:" + event + "\ndata:" + JSON.writeValueAsString(data) + "\n\n").getBytes(StandardCharsets.UTF_8));
            out.flush();
        }
    }

    private JsonNode stateJson() {
        synchronized (projection) {
            return projection.toJson().put("caughtUp", caughtUp);
        }
    }

    private JsonNode updateJson(String customer, OrderProjection.Stats stats) {
        synchronized (projection) {
            var update = JSON.createObjectNode().put("index", projection.index()).put("customer", customer);
            update.set("stats", JSON.valueToTree(stats));
            update.set("totals", JSON.valueToTree(projection.totals()));
            return update;
        }
    }

    private static String env(String name, String defaultValue) {
        var value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
