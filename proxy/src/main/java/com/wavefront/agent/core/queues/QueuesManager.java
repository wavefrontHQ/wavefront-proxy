package com.wavefront.agent.core.queues;

import static com.wavefront.agent.api.APIContainer.CENTRAL_TENANT_NAME;

import com.wavefront.agent.core.buffers.Buffer;
import com.wavefront.agent.core.buffers.BuffersManager;
import com.wavefront.agent.core.senders.SenderTasksManager;
import com.wavefront.agent.data.EntityPropertiesFactory;
import com.wavefront.data.ReportableEntityType;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QueuesManager {
  private static Map<String, QueueInfo> queues = new ConcurrentHashMap<>();
  private static Map<String, EntityPropertiesFactory> entityProperties;

  public static void init(Map<String, EntityPropertiesFactory> entityPropertiesFactoryMap) {
    entityProperties = entityPropertiesFactoryMap;
  }

  public static QueueInfo initQueue(ReportableEntityType entityType) {
    return initQueue(entityType, CENTRAL_TENANT_NAME);
  }

  public static QueueInfo initQueue(ReportableEntityType entityType, String tenant) {
    QueueInfo q =
        new Q(entityType, tenant, entityProperties.get(tenant).get(entityType).getFlushThreads());
    queues.computeIfAbsent(
        q.getName(),
        s -> {
          setupQueue(q);
          return q;
        });
    return q;
  }

  private static void setupQueue(QueueInfo q) {
    List<Buffer> buffers = BuffersManager.registerNewQueueIfNeedIt(q);
    buffers.forEach(buffer -> SenderTasksManager.createSenderTasks(q, buffer, 1));
  }
}

class Q implements QueueInfo {
  private final String name;
  private final ReportableEntityType entityType;
  private final String tenant;
  private final int threads;

  Q(ReportableEntityType entityType, String tenant, int threads) {
    this.name = entityType + (tenant.equalsIgnoreCase(CENTRAL_TENANT_NAME) ? "" : "." + tenant);
    this.entityType = entityType;
    this.tenant = tenant;
    this.threads = threads;
  }

  public String getTenant() {
    return tenant;
  }

  public ReportableEntityType getEntityType() {
    return entityType;
  }

  public String getName() {
    return name;
  }

  @Override
  public int getNumberThreads() {
    return threads;
  }
}
