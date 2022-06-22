package com.wavefront.agent.buffer;

import com.wavefront.data.ReportableEntityType;

public interface QueueInfo {
  String getQueue();

  ReportableEntityType getEntityType();

  String getTenantName();
}
