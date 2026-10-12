package com.fifteen02.eventstore.controller;

import com.fifteen02.eventstore.config.InterfaceType;
import com.fifteen02.eventstore.config.InterfacesConfigProperties;
import com.fifteen02.eventstore.service.IndexService;
import com.fifteen02.eventstore.service.InterfaceStateService;
import com.fifteen02.eventstore.service.OpenStreams;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * What the debug console shows at the top: where the log is, how far behind each configured
 * interface is, and which HTTP streams are open. Realtime interfaces keep no position, so their
 * position and lag are null.
 */
@RestController
public class DebugOverviewController {

    private final IndexService indexService;
    private final InterfaceStateService interfaceStateService;
    private final InterfacesConfigProperties interfacesConfig;
    private final OpenStreams openStreams;
    private final boolean amqpEnabled;

    public DebugOverviewController(IndexService indexService,
                                   InterfaceStateService interfaceStateService,
                                   InterfacesConfigProperties interfacesConfig,
                                   OpenStreams openStreams,
                                   @Value("${eventstore.amqp.enabled:false}") boolean amqpEnabled) {
        this.indexService = indexService;
        this.interfaceStateService = interfaceStateService;
        this.interfacesConfig = interfacesConfig;
        this.openStreams = openStreams;
        this.amqpEnabled = amqpEnabled;
    }

    @GetMapping("/api/debug/overview")
    public Overview overview() {
        long head = indexService.current().value();
        var interfaces = interfacesConfig.getInterfaces().stream()
                .map(def -> interfaceView(def, head))
                .toList();
        var streams = openStreams.list().stream()
                .map(stream -> streamView(stream, head))
                .toList();
        return new Overview(head, amqpEnabled, interfaces, streams);
    }

    private InterfaceView interfaceView(InterfacesConfigProperties.InterfaceDefinition def, long head) {
        var type = def.getType() == null ? InterfaceType.realtime : def.getType();
        Long position = null;
        Long lag = null;
        Long ackBatch = null;
        if (type != InterfaceType.realtime) {
            position = interfaceStateService.getLastAck(def.getName());
            lag = Math.max(head - position, 0);
            ackBatch = (def.getAck() == null ? new InterfacesConfigProperties.Ack() : def.getAck()).getBatch();
        }
        return new InterfaceView(def.getName(), type.name(), sorted(def.getIncludeTypes()), sorted(def.getExcludeTypes()),
                interfaceStateService.isRunning(def.getName()), position, lag, ackBatch);
    }

    private static StreamView streamView(OpenStreams.Stream stream, long head) {
        var p = stream.parameters();
        long position = stream.position().get();
        return new StreamView(stream.id().substring(0, 8), stream.client(), stream.remoteAddress(), stream.openedAt(),
                position, Math.max(head - position, 0), p.startIndex(), p.endIndex(),
                sorted(p.messageType()), sorted(p.userIds()), p.shardIndex(), p.shardCount());
    }

    private static Set<String> sorted(Set<String> types) {
        return types == null ? Set.of() : new TreeSet<>(types);
    }

    public record Overview(long headIndex, boolean amqpEnabled, List<InterfaceView> interfaces, List<StreamView> streams) {
    }

    /**
     * @param position the last index the stream has passed, whether or not its filters let the message through
     */
    public record StreamView(String id, String client, String remoteAddress, Instant openedAt, long position, long lag,
                             Long startIndex, Long endIndex, Set<String> messageTypes, Set<String> userIds,
                             Integer shardIndex, Integer shardCount) {
    }

    public record InterfaceView(String name, String type, Set<String> includeTypes, Set<String> excludeTypes,
                                boolean running, Long position, Long lag, Long ackBatch) {
    }
}
