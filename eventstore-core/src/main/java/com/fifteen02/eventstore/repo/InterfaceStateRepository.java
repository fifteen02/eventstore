package com.fifteen02.eventstore.repo;

import com.fifteen02.eventstore.entity.InterfaceStateEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface InterfaceStateRepository extends JpaRepository<InterfaceStateEntity, String> {
    @Query("select min(i.lastAcknowledgedId) from InterfaceStateEntity i")
    Optional<Long> findMinLastAcknowledgedId();
}
