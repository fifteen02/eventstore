package com.fifteen02test.eventstore;

import com.fifteen02.eventstore.entity.Message;
import com.fifteen02.eventstore.entity.PersisterStateEntity;
import com.fifteen02.eventstore.repo.MessageRepository;
import com.fifteen02.eventstore.repo.PersisterStateRepository;
import com.fifteen02.eventstore.service.RealtimeRetentionJob;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

@SpringBootConfiguration
@EnableAutoConfiguration
@EntityScan(basePackageClasses = {Message.class, PersisterStateEntity.class})
@EnableJpaRepositories(basePackageClasses = {MessageRepository.class, PersisterStateRepository.class})
public class RealtimeRetentionJobTestApplication {

    @Bean
    Clock clock() {
        return Clock.fixed(Instant.parse("2026-03-31T08:00:00Z"), ZoneOffset.UTC);
    }

    @Bean
    RealtimeRetentionJob realtimeRetentionJob(PersisterStateRepository persisterStateRepository,
                                              MessageRepository messageRepository,
                                              Clock clock) {
        return new RealtimeRetentionJob(
                persisterStateRepository,
                messageRepository,
                clock,
                123,
                7,
                true);
    }
}
