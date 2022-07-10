package com.wavefront.agent.core.queues;

import static com.wavefront.agent.api.APIContainer.CENTRAL_TENANT_NAME;

import com.wavefront.data.ReportableEntityType;
import java.util.HashMap;
import java.util.Map;

class Queue implements QueueInfo {
  private final String name;
  private final ReportableEntityType entityType;
  private final String tenant;
  private final int threads;
  private final Map<String, QueueInfo> tenants = new HashMap<>();

  Queue(ReportableEntityType entityType, String tenant, int threads) {
    this.name = entityType + (tenant.equalsIgnoreCase(CENTRAL_TENANT_NAME) ? "" : "." + tenant);
    this.entityType = entityType;
    this.tenant = tenant;
    this.threads = threads;
  }

  public String getTenant() {
    return tenant;
  }

  @Override
  public QueueInfo getTenantQueue(String tenant) {
    return tenants.get(tenant);
  }

  @Override
  public Map<String, QueueInfo> getTenants() {
    return tenants;
  }

  public ReportableEntityType getEntityType() {
    return entityType;
  }

  public String getName() {
    return name;
  }

  @Override
  public int getNumberThreads() {
    return threads;
  }

  public void addTenant(String tenant, Queue queue) {
    tenants.put(tenant, queue);
  }
}
