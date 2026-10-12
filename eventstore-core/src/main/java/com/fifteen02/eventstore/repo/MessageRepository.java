package com.fifteen02.eventstore.repo;

import com.fifteen02.eventstore.entity.Message;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface MessageRepository extends JpaRepository<Message, Long> {

  // JpaRepository already provides findById(Long), keep no UUID overload

  @Query("select m from Message m where m.id between ?1 and ?2 order by m.id asc")
  List<Message> findRange(long fromInclusive, long toInclusive);

  @Query("select m.id from Message m order by m.id asc")
  List<Long> findAllIdsOrdered();

  @Query("select coalesce(max(m.id),0) from Message m")
  Long findMaxId();

  @Query("select coalesce(min(m.id),0) from Message m where m.timestamp >= ?1")
  Long findMinIdFromTimestamp(Instant timestamp);

  @Query("select m.id from Message m where m.id between ?1 and ?2 order by m.id asc")
  List<Long> findIdsInRange(long fromInclusive, long toInclusive, Pageable pageable);

  @Query("select m from Message m order by m.id desc")
  List<Message> findTopNByOrderByIdDesc(Pageable pageable);

  @Modifying
  @Query("delete from Message m where m.id in ?1")
  int deleteByIdIn(List<Long> ids);
}
