package com.fifteen02.eventstore.repo;

import com.fifteen02.eventstore.entity.PersisterStateEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface PersisterStateRepository extends JpaRepository<PersisterStateEntity, String> {

    @Query("""
              select p from PersisterStateEntity p
               where p.realtime = true
               order by p.priority desc
               limit 1
            """)
    Optional<PersisterStateEntity> findRealtime();

    @Query("""
              select p from PersisterStateEntity p
               where p.canRead = true and (p.lastIndex is not null or p.realtime = true)
               order by p.priority desc
            """)
    List<PersisterStateEntity> findAllReadableOrderByPriorityDesc();

    @Query("""
              select p from PersisterStateEntity p
               where p.realtime = false
            """)
    List<PersisterStateEntity> findAllNonRealtime();

}
