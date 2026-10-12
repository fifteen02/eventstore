package com.fifteen02.eventstore.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "persister_state")
public class PersisterStateEntity {
  @Id
  @Column(name = "id", nullable = false)
  private String id;

  @Column(name = "start_index", nullable = false)
  private Long startIndex;

  @Column(name = "last_index")
  private Long lastIndex;

  @Column(name = "can_read", nullable = false)
  private boolean canRead;

  @Column(name = "can_write", nullable = false)
  private boolean canWrite;

  @Column(name = "realtime", nullable = false)
  private boolean realtime;

  @Column(name = "priority", nullable = false)
  private Integer priority;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Long getStartIndex() {
    return startIndex;
  }

  public void setStartIndex(Long startIndex) {
    this.startIndex = startIndex;
  }

  public Long getLastIndex() {
    return lastIndex;
  }

  public void setLastIndex(Long lastIndex) {
    this.lastIndex = lastIndex;
  }

  public boolean isCanRead() {
    return canRead;
  }

  public void setCanRead(boolean canRead) {
    this.canRead = canRead;
  }

  public boolean isCanWrite() {
    return canWrite;
  }

  public void setCanWrite(boolean canWrite) {
    this.canWrite = canWrite;
  }

  public boolean isRealtime() {
    return realtime;
  }

  public void setRealtime(boolean realtime) {
    this.realtime = realtime;
  }

  public Integer getPriority() {
    return priority;
  }

  public void setPriority(Integer priority) {
    this.priority = priority;
  }
}
