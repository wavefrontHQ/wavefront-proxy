package com.wavefront.agent;

import com.google.common.collect.Maps;
import com.wavefront.agent.api.APIContainer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.TestOnly;

public class TokenManager {
  private static final Map<String, TenantInfo> multicastingTenantList = Maps.newHashMap();
  private static List<TokenWorker.Scheduled> scheduledWorkers = new ArrayList();
  private static List<TokenWorker.External> externalWorkers = new ArrayList();

  public static void addTenant(String tenantName, TenantInfo tokenWorker) {
    multicastingTenantList.put(tenantName, tokenWorker);

    if (tokenWorker instanceof TokenWorker.Scheduled) {
      scheduledWorkers.add((TokenWorker.Scheduled) tokenWorker);
    }

    if (tokenWorker instanceof TokenWorker.External) {
      externalWorkers.add((TokenWorker.External) tokenWorker);
    }
  }

  public static void start(APIContainer apiContainer) {
    externalWorkers.forEach(external -> external.setAPIContainer(apiContainer));
    scheduledWorkers.forEach(tenantInfo -> tenantInfo.run());
  }

  public static Map<String, TenantInfo> getMulticastingTenantList() {
    return multicastingTenantList;
  }

  @TestOnly
  public static void reset() {
    externalWorkers.clear();
    scheduledWorkers.clear();
    multicastingTenantList.clear();
  }
}
