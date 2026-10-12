package com.fifteen02.eventstore.pekko;

import com.fifteen02.eventstore.pekko.actors.RouterActor;
import com.fifteen02.eventstore.pekko.actors.interfaces.Interface;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ActorRegistry {

    private volatile ActorSystem<?> system;
    private volatile ActorRef<RouterActor.Command> router;
    private final Map<String, ActorRef<Interface.Command>> interfaces = new ConcurrentHashMap<>();
    private final AtomicBoolean shuttingDown = new AtomicBoolean();

    public void initialize(ActorSystem<?> system, ActorRef<RouterActor.Command> router) {
        this.system = system;
        this.router = router;
    }

    public void setSystem(ActorSystem<?> system) {
        this.system = system;
    }

    /**
     * True once the router is running. The web server takes requests a moment before that, and a
     * command sent to the router before then is dropped.
     */
    public boolean isReady() {
        return router != null && !shuttingDown.get();
    }

    public void tellRouter(RouterActor.Command msg) {
        if (router != null) router.tell(msg);
    }

    public boolean isShuttingDown() {
        return shuttingDown.get();
    }

    @PreDestroy
    public void shutdown() {
        if (!shuttingDown.compareAndSet(false, true) || system == null) {
            return;
        }

        system.terminate();
        try {
            system.getWhenTerminated()
                    .toCompletableFuture()
                    .get(Duration.ofSeconds(5).toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException exception) {
            // Context shutdown should continue even if the actor system takes longer than expected.
        } finally {
            router = null;
            interfaces.clear();
            system = null;
        }
    }
}
