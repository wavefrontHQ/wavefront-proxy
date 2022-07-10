package com.wavefront.agent;

import static com.wavefront.agent.api.APIContainer.CENTRAL_TENANT_NAME;
import static com.wavefront.data.ReportableEntityType.POINT;

import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.data.ReportableEntityType;
import java.util.HashMap;
import java.util.Map;

public class TestQueue implements QueueInfo {
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
    return POINT.name();
  }

  @Override
  public int getNumberThreads() {
    return 1;
  }
}
