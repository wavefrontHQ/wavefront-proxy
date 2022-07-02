package com.wavefront.agent.core.queues;

import com.wavefront.data.ReportableEntityType;

public interface QueueInfo {
  public String getTenant();

  public ReportableEntityType getEntityType();

  public String getName();
}
