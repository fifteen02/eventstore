package com.fifteen02;

import com.fifteen02.eventstore.config.InterfacesConfigProperties;
import com.fifteen02.eventstore.pekko.ActorRegistry;
import com.fifteen02.eventstore.pekko.actors.RouterActor;
import com.fifteen02.eventstore.service.CatchUpPlannerService;
import com.fifteen02.eventstore.service.IndexService;
import com.fifteen02.eventstore.service.InterfaceStateService;
import com.fifteen02.eventstore.service.PersistorService;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.amqp.autoconfigure.RabbitAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.EnableScheduling;
import scala.compat.java8.FutureConverters;
import tools.jackson.databind.ObjectMapper;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SpringBootApplication(exclude = RabbitAutoConfiguration.class)
@EnableConfigurationProperties({ InterfacesConfigProperties.class, RabbitProperties.class })
@EnableScheduling
@Order(3)
public class Application implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(Application.class);
    private static final Duration ROUTER_REGISTRATION_TIMEOUT = Duration.ofSeconds(30);

    private final InterfacesConfigProperties interfacesConfig;
    private final InterfaceStateService interfaceStateService;
    private final Optional<RabbitTemplate> rabbitTemplate;
    private final ActorRegistry actorRegistry;
    private final CatchUpPlannerService catchUpPlannerService;
    private final IndexService indexService;
    private final Optional<RabbitListenerEndpointRegistry> rabbitListenerEndpointRegistry;
    private final Optional<ConnectionFactory> connectionFactory;
    private final PersistorService persistorService;
    private final boolean amqpEnabled;
    private final Duration heartbeatInterval;
    private final ObjectMapper objectMapper;

    public Application(
            @Autowired InterfacesConfigProperties interfacesConfig,
            @Autowired InterfaceStateService interfaceStateService,
            @Autowired PersistorService persistorService,
            ObjectProvider<RabbitTemplate> rabbitTemplate,
            @Autowired ActorRegistry actorRegistry,
            @Autowired CatchUpPlannerService catchUpPlannerService,
            @Autowired IndexService indexService,
            ObjectProvider<RabbitListenerEndpointRegistry> rabbitListenerEndpointRegistry,
            ObjectProvider<ConnectionFactory> connectionFactory,
            @Autowired ObjectMapper objectMapper,
            @Value("${eventstore.amqp.enabled:false}") boolean amqpEnabled,
            @Value("${eventstore.sse.heartbeat-interval-seconds:5}") long heartbeatIntervalSeconds) {
        this.interfacesConfig = interfacesConfig;
        this.interfaceStateService = interfaceStateService;
        this.persistorService = persistorService;
        this.rabbitTemplate = Optional.ofNullable(rabbitTemplate.getIfAvailable());
        this.actorRegistry = actorRegistry;
        this.catchUpPlannerService = catchUpPlannerService;
        this.indexService = indexService;
        this.rabbitListenerEndpointRegistry = Optional.ofNullable(rabbitListenerEndpointRegistry.getIfAvailable());
        this.connectionFactory = Optional.ofNullable(connectionFactory.getIfAvailable());
        this.objectMapper = objectMapper;
        this.amqpEnabled = amqpEnabled;
        this.heartbeatInterval = Duration.ofSeconds(Math.max(1L, heartbeatIntervalSeconds));
    }

    @Override
    public void run(ApplicationArguments args) {

        String serverCorrelationUUID = UUID.randomUUID().toString();
        CompletableFuture<Void> routerRegistered = new CompletableFuture<>();
        // Initialize ActorSystem and spawn router with children according to
        // configuration
        ActorSystem<Void> system = ActorSystem.create(Behaviors.setup(ctx -> {
            ActorRef<RouterActor.Command> router = ctx.spawn(
                    RouterActor.create(serverCorrelationUUID, interfacesConfig, interfaceStateService, persistorService, rabbitTemplate,
                            catchUpPlannerService, indexService, heartbeatInterval, connectionFactory, objectMapper, amqpEnabled),
                    "router");
            actorRegistry.initialize(ctx.getSystem(), router);
            routerRegistered.complete(null);
            log.info("Pekko ActorSystem initialized, router spawned with {} interface(s) [CorrelationID:{}]",
                    interfacesConfig.getInterfaces().size(), serverCorrelationUUID);
            return Behaviors.empty();
        }), "eventstore-system");
        actorRegistry.setSystem(system);

        // If Pekko dies its never a good thing mate!
        var cs = FutureConverters.toJava(system.whenTerminated());
        cs.thenRun(() -> {
            if (!actorRegistry.isShuttingDown()) {
                System.out.println("System terminated");
                System.exit(-1);
            }
        });

        if (amqpEnabled) {
            // A message consumed before the router is registered is stored but never routed to realtime
            // interfaces, so only start consuming once it is.
            awaitRouter(routerRegistered);
            rabbitListenerEndpointRegistry.ifPresent(RabbitListenerEndpointRegistry::start);
            log.info("AMQP input/output enabled");
        } else {
            log.info("AMQP input/output disabled");
        }
    }

    private static void awaitRouter(CompletableFuture<Void> routerRegistered) {
        try {
            routerRegistered.get(ROUTER_REGISTRATION_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the router to start", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Router did not start within " + ROUTER_REGISTRATION_TIMEOUT
                    + "; not starting the AMQP input listener", e);
        }
    }

    @PreDestroy
    public void shutdown() {
        actorRegistry.shutdown();
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
