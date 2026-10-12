package com.fifteen02;

import com.fifteen02.eventstore.config.InterfaceType;
import com.fifteen02.eventstore.config.InterfacesConfigProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Component
@ConditionalOnProperty(name = "eventstore.amqp.enabled", havingValue = "true")
@Order(1)
public class AmqpInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(AmqpInitializer.class);

    private final InterfacesConfigProperties interfacesConfigProperties;
    private final ConnectionFactory defaultConnectionFactory;
    private final AmqpAdmin defaultAmqpAdmin;
    private final RabbitProperties rabbitProperties;

    public AmqpInitializer(
            @Autowired InterfacesConfigProperties interfacesConfigProperties,
            @Autowired ConnectionFactory defaultConnectionFactory,
            @Autowired AmqpAdmin defaultAmqpAdmin,
            @Autowired RabbitProperties rabbitProperties) {
        this.interfacesConfigProperties = interfacesConfigProperties;
        this.defaultConnectionFactory = defaultConnectionFactory;
        this.defaultAmqpAdmin = defaultAmqpAdmin;
        this.rabbitProperties = rabbitProperties;
    }

    @Override
    public void run(ApplicationArguments args) {
        // Declare the input queue for eventstore
        declareInputQueue();

        // Declare queues for sinks and acks using default direct exchange ("")
        for (InterfacesConfigProperties.InterfaceDefinition def : interfacesConfigProperties.getInterfaces()) {
            declareQueuesFor(def);
        }
    }

    private void declareQueuesFor(InterfacesConfigProperties.InterfaceDefinition def) {
        // A persister's acknowledged range is what retention trusts as archived. Filtered messages would be
        // acknowledged without ever reaching the archive, and then deleted from PostgreSQL.
        if (def.getType() == InterfaceType.persister && (hasTypes(def.getIncludeTypes()) || hasTypes(def.getExcludeTypes()))) {
            throw new IllegalArgumentException("Persister interface [" + def.getName()
                    + "] must not set include-types or exclude-types: a persister has to archive every message.");
        }

        // Convention: <interface>.sink and <interface>.ack
        if (def.getSink() != null && def.getSink().getAmqp() != null) {
            InterfacesConfigProperties.Amqp amqp = def.getSink().getAmqp();
            declareQueue(def.getName() + ".sink", amqp.getVirtualHost(), "sink", def.getName());
        }

        if (def.getAck() != null && def.getType() == InterfaceType.realtime) {
            throw new IllegalArgumentException("Realtime interfaces do not support acks");
        }

        if (def.getAck() == null && def.getType() != InterfaceType.realtime) {
            throw new IllegalArgumentException("Interface [" + def.getName() + "] requires information on acknowledgments.  Please update.  ");
        }

        if (def.getAck() != null && def.getAck().getAmqp() != null) {
            InterfacesConfigProperties.Amqp amqp = def.getAck().getAmqp();
            declareQueue(def.getName() + ".ack", amqp.getVirtualHost(), "ack", def.getName());
        }
    }

    private static boolean hasTypes(Set<String> types) {
        return types != null && !types.isEmpty();
    }

    private void declareQueue(String queueName, String virtualHost, String purpose, String interfaceName) {
        if (queueName == null || queueName.isBlank()) {
            return;
        }
        try {
            AmqpAdmin adminToUse = resolveAdminForVirtualHost(virtualHost);
            adminToUse.declareQueue(new Queue(queueName, true));
            log.info("Declared {} queue '{}' for interface '{}'{}",
                    purpose,
                    queueName,
                    interfaceName,
                    virtualHost != null && !virtualHost.isBlank() ? " on vhost '" + virtualHost + "'" : "");
        } catch (Exception e) {
            log.error("Failed to declare {} queue '{}' for interface '{}': {}",
                    purpose, queueName, interfaceName, e.getMessage(), e);
        }
    }

    private AmqpAdmin resolveAdminForVirtualHost(String virtualHost) {
        if (virtualHost == null || virtualHost.isBlank()) {
            return defaultAmqpAdmin;
        }
        if (defaultConnectionFactory instanceof CachingConnectionFactory ccf) {
            // If the requested vhost matches the default, reuse admin
            if (virtualHost.equals(ccf.getVirtualHost())) {
                return defaultAmqpAdmin;
            }
        }
        // Create a temporary admin for that vhost using Spring Boot RabbitProperties
        CachingConnectionFactory perVhost = new CachingConnectionFactory();
        String addresses = rabbitProperties.getAddresses() == null ? "" : rabbitProperties.getAddresses().stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(address -> !address.isBlank())
                .collect(Collectors.joining(","));
        if (!addresses.isBlank()) {
            // If the addresses string is defined, prefer it
            perVhost.setAddresses(addresses);
        } else {
            perVhost.setHost(rabbitProperties.getHost());
            perVhost.setPort(rabbitProperties.getPort());
        }
        perVhost.setUsername(rabbitProperties.getUsername());
        perVhost.setPassword(rabbitProperties.getPassword());
        perVhost.setVirtualHost(virtualHost);
        return new RabbitAdmin(perVhost);
    }

    private void declareInputQueue() {
        // Get the input queue name from eventstore configuration
        String inputQueueName = interfacesConfigProperties.getInput().getQueue();
        if (inputQueueName == null || inputQueueName.isBlank()) {
            log.warn("No input queue configured, skipping input queue declaration");
            return;
        }

        // Use the default virtual host for the input queue
        String virtualHost = null;
        if (defaultConnectionFactory instanceof CachingConnectionFactory ccf) {
            virtualHost = ccf.getVirtualHost();
        }

        declareQueue(inputQueueName, virtualHost, "input", "eventstore");
    }
}
