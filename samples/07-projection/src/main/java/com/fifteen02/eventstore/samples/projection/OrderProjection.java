package com.fifteen02.eventstore.samples.projection;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.TreeMap;

/**
 * "Orders per customer": a fold over the order events in the log.
 *
 * <p>State is one {@link Stats} per customer (the cells), plus the open orders needed to undo a
 * cancellation. {@link #index} is the global index of the last event folded in; together with the
 * state it forms a snapshot: "this is the state after folding events 1..index".
 */
final class OrderProjection {

    /** Per-customer totals. */
    record Stats(int placed, int shipped, int cancelled, long spent) {
        static final Stats ZERO = new Stats(0, 0, 0, 0);
    }

    /** An order we may still need to refund. */
    record OpenOrder(String customer, long total) {
    }

    private static final ObjectMapper JSON = new ObjectMapper();

    private long index;
    private final Map<String, Stats> customers = new TreeMap<>();
    private final Map<String, OpenOrder> openOrders = new TreeMap<>();

    long index() {
        return index;
    }

    Stats stats(String customer) {
        return customers.getOrDefault(customer, Stats.ZERO);
    }

    /**
     * Folds one event into the state. Returns the customer whose cell changed, or null when the
     * event does not touch this projection. Events at or below {@link #index} are ignored, which
     * makes re-delivered events harmless.
     */
    String apply(long id, String type, String customer, JsonNode body) {
        if (id <= index) {
            return null;                                   // already folded: idempotent by index
        }
        index = id;
        if (customer == null || customer.isBlank()) {
            return null;
        }
        String orderId = body.path("orderId").asString();
        Stats s = stats(customer);
        switch (type) {
            case "OrderPlaced" -> {
                long total = body.path("total").asLong();
                openOrders.put(orderId, new OpenOrder(customer, total));
                s = new Stats(s.placed() + 1, s.shipped(), s.cancelled(), s.spent() + total);
            }
            case "OrderShipped" -> s = new Stats(s.placed(), s.shipped() + 1, s.cancelled(), s.spent());
            case "OrderCancelled" -> {
                // Order-sensitive: a cancellation refunds a total we learned from an earlier event.
                OpenOrder order = openOrders.remove(orderId);
                long refund = order == null ? 0 : order.total();
                s = new Stats(s.placed(), s.shipped(), s.cancelled() + 1, s.spent() - refund);
            }
            default -> {
                return null;
            }
        }
        customers.put(customer, s);
        return customer;
    }

    /** A coarser view derived from the per-customer cells: sum them up, no events re-read. */
    Stats totals() {
        int placed = 0, shipped = 0, cancelled = 0;
        long spent = 0;
        for (Stats s : customers.values()) {
            placed += s.placed();
            shipped += s.shipped();
            cancelled += s.cancelled();
            spent += s.spent();
        }
        return new Stats(placed, shipped, cancelled, spent);
    }

    ObjectNode toJson() {
        ObjectNode root = JSON.createObjectNode();
        root.put("index", index);
        root.set("customers", JSON.valueToTree(customers));
        root.set("totals", JSON.valueToTree(totals()));
        return root;
    }

    /** Writes index and state together, atomically: a snapshot is never half-written. */
    void save(Path file) throws Exception {
        ObjectNode snapshot = JSON.createObjectNode();
        snapshot.put("index", index);
        snapshot.set("customers", JSON.valueToTree(customers));
        snapshot.set("openOrders", JSON.valueToTree(openOrders));
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        Files.writeString(tmp, JSON.writeValueAsString(snapshot));
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    static OrderProjection load(Path file) throws Exception {
        var projection = new OrderProjection();
        if (!Files.exists(file)) {
            return projection;
        }
        JsonNode snapshot = JSON.readTree(Files.readString(file));
        projection.index = snapshot.path("index").asLong();
        snapshot.path("customers").properties().forEach(e ->
                projection.customers.put(e.getKey(), JSON.treeToValue(e.getValue(), Stats.class)));
        snapshot.path("openOrders").properties().forEach(e ->
                projection.openOrders.put(e.getKey(), JSON.treeToValue(e.getValue(), OpenOrder.class)));
        return projection;
    }
}
