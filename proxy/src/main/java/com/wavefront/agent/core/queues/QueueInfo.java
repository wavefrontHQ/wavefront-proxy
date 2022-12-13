package com.wavefront.agent.core.queues;

import com.wavefront.data.ReportableEntityType;
import java.util.Map;

public interface QueueInfo {
  String getTenant();

  QueueInfo getTenantQueue(String tenant);

  Map<String, QueueInfo> getTenants();

  ReportableEntityType getEntityType();

  String getName();

  int getNumberThreads();

  int getMaxItemsPerMessage();
}
