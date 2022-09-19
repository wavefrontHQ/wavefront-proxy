package com.wavefront.agent.core.queues;

import static com.wavefront.agent.api.APIContainer.CENTRAL_TENANT_NAME;

import com.wavefront.data.ReportableEntityType;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TestQueue implements QueueInfo {
  private final int idx;
  private static AtomicInteger i = new AtomicInteger(0);
  private final int threads;
  private ReportableEntityType entityType;

  public TestQueue(ReportableEntityType entityType) {
    this(1, entityType);
  }

  public TestQueue(int threads, ReportableEntityType entityType) {
    this.entityType = entityType;
    idx = i.getAndIncrement();
    this.threads = threads;
    QueueStats.register(this);
  }

  @Override
  public String getTenant() {
    return CENTRAL_TENANT_NAME;
  }

  @Override
  public QueueInfo getTenantQueue(String tenant) {
    return null;
  }

  @Override
  public Map<String, QueueInfo> getTenants() {
    return new HashMap<>();
  }

  @Override
  public ReportableEntityType getEntityType() {
    return this.entityType;
  }

  @Override
  public String getName() {
    return getEntityType().name() + "_" + idx;
  }

  @Override
  public int getNumberThreads() {
    return threads;
  }
}
