package com.wavefront.agent.handlers;

import static com.wavefront.agent.api.APIContainer.CENTRAL_TENANT_NAME;

import com.wavefront.agent.buffer.QueueInfo;
import com.wavefront.data.ReportableEntityType;

/**
 * An immutable unique identifier for a handler pipeline (type of objects handled + port/handle name
 * + tenant name)
 */
public class HandlerKey implements QueueInfo {

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
    this.tenantName = tenantName == null ? CENTRAL_TENANT_NAME : tenantName;
    queue =
        entityType + ((CENTRAL_TENANT_NAME.equals(this.tenantName)) ? "" : "." + this.tenantName);
  }

  @Override
  public String getQueue() {
    return queue;
  }

  @Override
  public ReportableEntityType getEntityType() {
    return entityType;
  }

  public String getPort() {
    return port;
  }

  @Override
  public String getTenantName() {
    return tenantName;
  }
}
