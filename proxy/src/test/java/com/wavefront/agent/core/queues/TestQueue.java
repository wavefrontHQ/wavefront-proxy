package com.wavefront.agent.core.queues;

import static com.wavefront.agent.api.APIContainer.CENTRAL_TENANT_NAME;

import com.wavefront.data.ReportableEntityType;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TestQueue implements QueueInfo {
  private static AtomicInteger i = new AtomicInteger(0);
  private final int idx;
  private final int threads;
  private final ReportableEntityType entityType;
  private final boolean index; // index is used to have different names to allow multiple tests
  public int itemsPM;

  public TestQueue(ReportableEntityType entityType) {
    this(1, entityType, true);
  }

  public TestQueue(ReportableEntityType entityType, boolean index) {
    this(1, entityType, index);
  }

  public TestQueue(int threads, ReportableEntityType entityType) {
    this(threads, entityType, true);
  }

  public TestQueue(int threads, ReportableEntityType entityType, boolean index) {
    this.entityType = entityType;
    idx = i.getAndIncrement();
    this.threads = threads;
    this.index = index;
    itemsPM = 1;
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
    return getEntityType().name() + (index ? "_" + idx : "");
  }

  @Override
  public int getNumberThreads() {
    return threads;
  }

  public int getMaxItemsPerMessage() {
    return itemsPM;
  }
}
