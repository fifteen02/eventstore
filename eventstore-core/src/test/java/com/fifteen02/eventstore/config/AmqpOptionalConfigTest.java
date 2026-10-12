package com.fifteen02.eventstore.config;

import com.fifteen02.Application;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.amqp.autoconfigure.RabbitAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class AmqpOptionalConfigTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(AmqpConfig.class);

    @Test
    void applicationExcludesBootRabbitAutoConfiguration() {
        var annotation = Application.class.getAnnotation(SpringBootApplication.class);

        assertThat(annotation.exclude()).contains(RabbitAutoConfiguration.class);
    }

    @Test
    void doesNotCreateRabbitBeansByDefault() {
        contextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(ConnectionFactory.class);
            assertThat(context).doesNotHaveBean(RabbitTemplate.class);
            assertThat(context).doesNotHaveBean(AmqpAdmin.class);
        });
    }

    @Test
    void createsRabbitBeansWhenAmqpIsEnabled() {
        contextRunner
                .withPropertyValues("eventstore.amqp.enabled=true")
                .run(context -> {
                    assertThat(context).hasSingleBean(ConnectionFactory.class);
                    assertThat(context).hasSingleBean(RabbitTemplate.class);
                    assertThat(context).hasSingleBean(AmqpAdmin.class);
                });
    }
}
