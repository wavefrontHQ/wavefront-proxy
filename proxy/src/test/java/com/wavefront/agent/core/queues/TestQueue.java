package com.wavefront.agent.core.queues;

import static com.wavefront.agent.api.APIContainer.CENTRAL_TENANT_NAME;
import static com.wavefront.data.ReportableEntityType.POINT;

import com.wavefront.data.ReportableEntityType;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TestQueue implements QueueInfo {
  private int idx;
  private static AtomicInteger i = new AtomicInteger(0);
  private int threads;

  public TestQueue() {
    this(1);
  }

  public TestQueue(int threads) {
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
    return POINT;
  }

  @Override
  public String getName() {
    return POINT.name() + "_" + idx;
  }

  @Override
  public int getNumberThreads() {
    return threads;
  }
}
