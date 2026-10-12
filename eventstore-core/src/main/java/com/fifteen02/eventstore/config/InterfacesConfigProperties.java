package com.fifteen02.eventstore.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * {@code eventstore.*} properties: duplicate-suppression lookback, the AMQP input queue and the
 * list of AMQP interfaces. See {@code docs/configuration.md} for every option and its default.
 */
@ConfigurationProperties(prefix = "eventstore")
public class InterfacesConfigProperties {
    private Integer lookback;
    private final Input input = new Input();
    private final List<InterfaceDefinition> interfaces = new ArrayList<>();

    public void setLookback(Integer lookback) {
        this.lookback = lookback;
    }

    /**
     * Number of recent content hashes checked for duplicates during the first {@code lookback}
     * messages after startup. Defaults to 100; 0 disables duplicate suppression.
     */
    public Integer getLookback() {
        if (lookback == null) {
            return 100;
        }
        return lookback;
    }

    public Input getInput() {
        return input;
    }

    public List<InterfaceDefinition> getInterfaces() {
        return interfaces;
    }

    /** {@code eventstore.input}: the AMQP queue producers publish to. */
    public static class Input {
        private String queue;

        public String getQueue() {
            return queue;
        }

        public void setQueue(String queue) {
            this.queue = queue;
        }
    }

    /**
     * One entry of {@code eventstore.interfaces}. EventStore publishes selected messages to
     * {@code <name>.sink} and, for {@code replayable} and {@code persister} types, consumes
     * acknowledgements from {@code <name>.ack}.
     */
    public static class InterfaceDefinition {
        private String name;
        private InterfaceType type; // realtime | replayable | persister
        private Set<String> includeTypes;
        private Set<String> excludeTypes;
        private Sink sink;
        private Ack ack;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public InterfaceType getType() {
            return type;
        }

        public void setType(InterfaceType type) {
            this.type = type;
        }

        public Set<String> getIncludeTypes() {
            return includeTypes;
        }

        public void setIncludeTypes(Set<String> includeTypes) {
            this.includeTypes = includeTypes;
        }

        public Set<String> getExcludeTypes() {
            return excludeTypes;
        }

        public void setExcludeTypes(Set<String> excludeTypes) {
            this.excludeTypes = excludeTypes;
        }

        public Sink getSink() {
            return sink;
        }

        public void setSink(Sink sink) {
            this.sink = sink;
        }

        public Ack getAck() {
            return ack;
        }

        public void setAck(Ack ack) {
            this.ack = ack;
        }
    }

    /** Declares {@code <name>.sink}. Always configure it, or the sink queue is never created. */
    public static class Sink {
        private Amqp amqp;

        public Amqp getAmqp() {
            return amqp;
        }

        public void setAmqp(Amqp amqp) {
            this.amqp = amqp;
        }
    }

    /** Acknowledgement settings. Required for {@code replayable} and {@code persister}; not allowed for {@code realtime}. */
    public static class Ack {
        private Amqp amqp;
        private Long batch;
        private Integer prefetch;
        private Double lagAlertBatchRatio;
        private Long lagAlertMinMessages;

        public Amqp getAmqp() {
            return amqp;
        }

        /** Persist the acknowledged position every this many contiguous indices. Defaults to 1000. */
        public Long getBatch() {
            return batch == null ? 1000L : batch;
        }

        /** Prefetch count of the {@code <name>.ack} consumer. Defaults to 1024. */
        public int getPrefetch() {
            if (prefetch == null) {
                return 1024;
            }
            return prefetch;
        }

        /**
         * Multiplier used by lag monitoring to derive an alert threshold from ack batch size.
         * Default keeps current behavior: 1.5 * batch.
         */
        public double getLagAlertBatchRatio() {
            if (lagAlertBatchRatio == null) {
                return 1.5d;
            }
            return lagAlertBatchRatio;
        }

        /**
         * Optional floor for lag alerting in absolute messages.
         * Default is one batch.
         */
        public long getLagAlertMinMessages() {
            if (lagAlertMinMessages == null) {
                return getBatch();
            }
            return lagAlertMinMessages;
        }

        public void setAmqp(Amqp amqp) {
            this.amqp = amqp;
        }

        public void setBatch(Long batch) {
            this.batch = batch;
        }

        public void setPrefetch(Integer prefetch) {
            this.prefetch = prefetch;
        }

        public void setLagAlertBatchRatio(Double lagAlertBatchRatio) {
            this.lagAlertBatchRatio = lagAlertBatchRatio;
        }

        public void setLagAlertMinMessages(Long lagAlertMinMessages) {
            this.lagAlertMinMessages = lagAlertMinMessages;
        }

    }

    /**
     * Where the queue is declared. Publishing and ack consumption always use
     * {@code spring.rabbitmq.virtual-host}, so keep the two equal.
     */
    public static class Amqp {
        private String virtualHost;

        public String getVirtualHost() {
            return virtualHost;
        }

        public void setVirtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
        }
    }
}
