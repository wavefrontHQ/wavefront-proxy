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
  private final int midBufferItems;

  Queue(ReportableEntityType entityType, String tenant, int threads) {
    this.name = entityType + (tenant.equalsIgnoreCase(CENTRAL_TENANT_NAME) ? "" : "." + tenant);
    this.entityType = entityType;
    this.tenant = tenant;
    this.threads = threads;
    switch (entityType) {
      case LOGS:
        midBufferItems = 10;
        break;
      case POINT:
        midBufferItems = 255;
        break;
      default:
        midBufferItems = 100;
    }
    QueueStats.register(this);
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

  @Override
  public int getMaxItemsPerMessage() {
    return midBufferItems;
  }

  public void addTenant(String tenant, Queue queue) {
    tenants.put(tenant, queue);
  }
}
