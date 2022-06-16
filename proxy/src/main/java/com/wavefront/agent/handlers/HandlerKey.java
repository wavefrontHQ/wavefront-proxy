package com.wavefront.agent.handlers;

import com.wavefront.data.ReportableEntityType;

/**
 * An immutable unique identifier for a handler pipeline (type of objects handled + port/handle name
 * + tenant name)
 */
public class HandlerKey {

  private final String queue;
  private final ReportableEntityType entityType;
  private final String port;
  private final String tenantName;

  public HandlerKey(ReportableEntityType entityType, String port) {
    this(entityType, port, null);
  }

  public HandlerKey(ReportableEntityType entityType, String port, String tenantName) {
    this.entityType = entityType;
    this.port = port;
    this.tenantName = tenantName;
    queue = entityType + "." + port + ((null != null) ? "." + tenantName : "");
  }

  public int hashCode() {
    return queue.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HandlerKey that = (HandlerKey) o;
    return queue.equals(that);
  }

  public String getQueue() {
    return queue;
  }

  public ReportableEntityType getEntityType() {
    return entityType;
  }

  public String getPort() {
    return port;
  }

  public String getTenantName() {
    return tenantName;
  }
}
