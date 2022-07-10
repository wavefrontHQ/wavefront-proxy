package com.wavefront.agent.core.queues;

import com.wavefront.data.ReportableEntityType;
import java.util.Map;

public interface QueueInfo {
  public String getTenant();

  public QueueInfo getTenantQueue(String tenant);

  public Map<String, QueueInfo> getTenants();

  public ReportableEntityType getEntityType();

  public String getName();

  public int getNumberThreads();
}
