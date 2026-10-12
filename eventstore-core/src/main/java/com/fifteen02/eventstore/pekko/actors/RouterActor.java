package com.fifteen02.eventstore.pekko.actors;

import com.fifteen02.eventstore.config.InterfacesConfigProperties;
import com.fifteen02.eventstore.controller.StreamRequestParameters;
import com.fifteen02.eventstore.pekko.actors.interfaces.Interface;
import com.fifteen02.eventstore.pekko.actors.interfaces.PersisterInterface;
import com.fifteen02.eventstore.pekko.actors.interfaces.RealtimeInterface;
import com.fifteen02.eventstore.pekko.actors.interfaces.ReplayableInterface;
import com.fifteen02.eventstore.pekko.actors.interfaces.StreamingInterface;
import com.fifteen02.eventstore.service.CatchUpPlannerService;
import com.fifteen02.eventstore.service.InterfaceStateService;
import com.fifteen02.eventstore.service.IndexService;
import com.fifteen02.eventstore.service.PersistorService;
import com.fifteen02.eventstore.to.MessageTo;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.SupervisorStrategy;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongConsumer;

public class RouterActor extends AbstractBehavior<RouterActor.Command> {

    static final Duration INTERFACE_RESTART_MIN_BACKOFF = Duration.ofSeconds(1);
    static final Duration INTERFACE_RESTART_MAX_BACKOFF = Duration.ofSeconds(30);

    private final Map<String, ActorRef<Interface.Command>> interfaces = new HashMap<>();
    private final Map<String, ActorRef<Interface.Command>> streamingInterfaces = new HashMap<>();
    private final CatchUpPlannerService catchUpPlannerService;
    private final IndexService indexService;
    private final Duration heartbeatInterval;
    private final ObjectMapper objectMapper;

    public sealed interface Command permits Route, RegisterSseStream, UnregisterSseStream {
    }

    public record Route(MessageTo messageTo) implements Command {
    }

    /**
     * @param onObserved told every index the stream passes, filtered out or not, so its position can be
     *                   shown while it is open
     */
    public record RegisterSseStream(String streamId,
                                    SseEmitter emitter,
                                    StreamRequestParameters requestParameters,
                                    LongConsumer onObserved) implements Command {

        public RegisterSseStream(String streamId, SseEmitter emitter, StreamRequestParameters requestParameters) {
            this(streamId, emitter, requestParameters, index -> {
            });
        }
    }

    public record UnregisterSseStream(String streamId) implements Command {
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Route.class, this::onRoute)
                .onMessage(RegisterSseStream.class, this::onRegisterSseStream)
                .onMessage(UnregisterSseStream.class, this::onUnregisterSseStream)
                .build();
    }

    public static Behavior<Command> create(String serverCorrelationUUID,
                                           InterfacesConfigProperties config,
                                           InterfaceStateService interfaceStateService,
                                           PersistorService persistorService,
                                           Optional<RabbitTemplate> rabbitTemplate,
                                           CatchUpPlannerService catchUpPlannerService,
                                           IndexService indexService,
                                           Duration heartbeatInterval,
                                           Optional<ConnectionFactory> connectionFactory,
                                           ObjectMapper objectMapper,
                                           boolean amqpEnabled) {
        return Behaviors.setup(ctx -> new RouterActor(ctx, serverCorrelationUUID, config, interfaceStateService, persistorService,
                rabbitTemplate, catchUpPlannerService, indexService, heartbeatInterval, connectionFactory, objectMapper,
                amqpEnabled));
    }

    private RouterActor(ActorContext<Command> ctx,
                        String correlationUUID,
                        InterfacesConfigProperties config,
                        InterfaceStateService interfaceStateService,
                        PersistorService persistorService,
                        Optional<RabbitTemplate> rabbitTemplate,
                        CatchUpPlannerService catchUpPlannerService,
                        IndexService indexService,
                        Duration heartbeatInterval,
                        Optional<ConnectionFactory> connectionFactory,
                        ObjectMapper objectMapper,
                        boolean amqpEnabled) {
        super(ctx);
        this.catchUpPlannerService = catchUpPlannerService;
        this.indexService = indexService;
        this.heartbeatInterval = heartbeatInterval;
        this.objectMapper = objectMapper;

        config.getInterfaces().stream()
                .flatMap(def -> spawnInterface(ctx, correlationUUID, interfaceStateService, persistorService, rabbitTemplate,
                        catchUpPlannerService, connectionFactory, amqpEnabled, def).stream())
                .forEach(interfaceRef -> interfaces.put(interfaceRef.name(), interfaceRef.ref()));
    }

    private Optional<InterfaceRef> spawnInterface(ActorContext<Command> ctx,
                                                  String correlationUUID,
                                                  InterfaceStateService interfaceStateService,
                                                  PersistorService persistorService,
                                                  Optional<RabbitTemplate> rabbitTemplate,
                                                  CatchUpPlannerService catchUpPlannerService,
                                                  Optional<ConnectionFactory> connectionFactory,
                                                  boolean amqpEnabled,
                                                  InterfacesConfigProperties.InterfaceDefinition def) {
        if (!amqpEnabled || rabbitTemplate.isEmpty()) {
            ctx.getLog().info("Skipping AMQP interface [{}] because AMQP is disabled", def.getName());
            return Optional.empty();
        }

        return switch (def.getType()) {
            case realtime -> Optional.of(new InterfaceRef(
                    def.getName(),
                    ctx.spawn(supervised(RealtimeInterface.create(correlationUUID, def, interfaceStateService,
                                    rabbitTemplate.get(), objectMapper)),
                            "realtime-" + def.getName())));
            case replayable -> connectionFactory.map(factory -> new InterfaceRef(
                    def.getName(),
                    ctx.spawn(supervised(ReplayableInterface.create(correlationUUID, def, interfaceStateService,
                            rabbitTemplate.get(), objectMapper, catchUpPlannerService, factory)), "replayable-" + def.getName())));
            case persister -> connectionFactory.map(factory -> new InterfaceRef(
                    def.getName(),
                    ctx.spawn(supervised(PersisterInterface.create(correlationUUID, def, interfaceStateService, persistorService,
                            rabbitTemplate.get(), objectMapper, catchUpPlannerService, factory)), "persister-" + def.getName())));
        };
    }

    /**
     * Restart an interface that fails (typically a publish while the broker is unreachable) instead of
     * letting it stop for good. The backoff keeps a broker outage from becoming a restart loop. A restarted
     * replayable or persister interface catches up again from its saved position, so nothing is lost; a
     * realtime interface resumes with new messages.
     */
    static Behavior<Interface.Command> supervised(Behavior<Interface.Command> behavior) {
        return Behaviors.supervise(behavior)
                .onFailure(Exception.class, SupervisorStrategy.restartWithBackoff(
                        INTERFACE_RESTART_MIN_BACKOFF, INTERFACE_RESTART_MAX_BACKOFF, 0.2));
    }

    private Behavior<Command> onRoute(Route route) {
        interfaces.values().forEach(ref -> ref.tell(new Interface.Routed(route.messageTo)));
        streamingInterfaces.values().forEach(ref -> ref.tell(new Interface.Routed(route.messageTo)));
        return this;
    }

    private Behavior<Command> onRegisterSseStream(RegisterSseStream command) {
        unregisterSseStream(command.streamId());
        var actorName = "streaming-interface-" + command.streamId().replace("-", "_");
        ActorRef<Interface.Command> interfaceRef = getContext().spawn(
                StreamingInterface.create(
                        sseDefinitionFor(command.streamId()),
                        catchUpPlannerService,
                        command.emitter(),
                        command.requestParameters(),
                        heartbeatInterval,
                        () -> indexService.current().value(),
                        command.onObserved()),
                actorName);
        streamingInterfaces.put(command.streamId(), interfaceRef);
        getContext().watchWith(interfaceRef, new UnregisterSseStream(command.streamId()));
        getContext().getLog().debug("Registered SSE stream [{}] with filters: {}",
                command.streamId(), command.requestParameters());
        return this;
    }

    private Behavior<Command> onUnregisterSseStream(UnregisterSseStream command) {
        if (unregisterSseStream(command.streamId())) {
            getContext().getLog().debug("Unregistered SSE stream [{}]", command.streamId());
        }
        return this;
    }

    private boolean unregisterSseStream(String streamId) {
        ActorRef<Interface.Command> streamingInterface = streamingInterfaces.remove(streamId);
        if (streamingInterface == null) {
            return false;
        }
        getContext().stop(streamingInterface);
        return true;
    }

    private InterfacesConfigProperties.InterfaceDefinition sseDefinitionFor(String streamId) {
        var definition = new InterfacesConfigProperties.InterfaceDefinition();
        definition.setName("stream-" + streamId);
        return definition;
    }

    private record InterfaceRef(String name, ActorRef<Interface.Command> ref) {
    }

}
