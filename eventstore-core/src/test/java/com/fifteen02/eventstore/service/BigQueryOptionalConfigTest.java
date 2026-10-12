package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.repo.MessageRepository;
import com.fifteen02.eventstore.repo.PersisterStateRepository;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Clock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class BigQueryOptionalConfigTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(
                    GoogleBigQueryClientImpl.class,
                    BigQueryMessageFetcher.class,
                    RealtimeRetentionJob.class
            )
            .withBean(PersisterStateRepository.class, () -> mock(PersisterStateRepository.class))
            .withBean(MessageRepository.class, () -> mock(MessageRepository.class))
            .withBean(Clock.class, Clock::systemUTC)
            .withPropertyValues(
                    "eventstore.realtime-retention.enabled=true",
                    "eventstore.realtime-retention.delete-batch-size=100",
                    "eventstore.realtime-retention.retention-days=7",
                    "eventstore.realtime-retention.cron=-"
            );

    @Test
    void doesNotRequireBigQueryPropertiesWhenBigQueryIsDisabled() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(GoogleBigQueryClient.class);
            assertThat(context).doesNotHaveBean(BigQueryMessageFetcher.class);
            assertThat(context).doesNotHaveBean(RealtimeRetentionJob.class);
        });
    }
}
