package com.wavefront.agent.core.queues;

import com.wavefront.data.ReportableEntityType;

public interface QueuesManager {
  public QueueInfo initQueue(ReportableEntityType entityType);
}
