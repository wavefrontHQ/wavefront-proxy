package com.wavefront.agent.core.queues;

import com.wavefront.data.ReportableEntityType;

public interface QueuesManager {
  QueueInfo initQueue(ReportableEntityType entityType);
}
