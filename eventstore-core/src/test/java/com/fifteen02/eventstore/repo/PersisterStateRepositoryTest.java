package com.fifteen02.eventstore.repo;

import com.fifteen02.eventstore.entity.PersisterStateEntity;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase.Replace;

@DataJpaTest(showSql = false, properties = {
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.url=jdbc:h2:mem:persister-state-repository;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE;INIT=CREATE DOMAIN IF NOT EXISTS JSONB AS JSON",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
        "spring.jpa.properties.hibernate.format_sql=false"
})
@Import(PersisterStateRepositoryTest.TestApplication.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
class PersisterStateRepositoryTest {

    @Autowired
    private PersisterStateRepository persisterStateRepository;

    @Test
    void findAllReadableOrderByPriorityDescIncludesRealtimeWithNullLastIndex() {
        persisterStateRepository.save(persister("archive", 1L, 14L, 10, false));
        persisterStateRepository.save(persister("writer-postgres", 1L, null, Integer.MAX_VALUE, true));

        assertThat(persisterStateRepository.findAllReadableOrderByPriorityDesc())
                .extracting(PersisterStateEntity::getId)
                .containsExactly("writer-postgres", "archive");
    }

    private static PersisterStateEntity persister(String id,
                                                  Long startIndex,
                                                  Long lastIndex,
                                                  int priority,
                                                  boolean realtime) {
        var entity = new PersisterStateEntity();
        entity.setId(id);
        entity.setStartIndex(startIndex);
        entity.setLastIndex(lastIndex);
        entity.setCanRead(true);
        entity.setCanWrite(true);
        entity.setRealtime(realtime);
        entity.setPriority(priority);
        return entity;
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EntityScan(basePackageClasses = PersisterStateEntity.class)
    @EnableJpaRepositories(basePackageClasses = PersisterStateRepository.class)
    static class TestApplication {
    }
}
