package com.wavefront.agent.core.queues;

import static com.wavefront.agent.api.APIContainer.CENTRAL_TENANT_NAME;

import com.wavefront.agent.core.buffers.Buffer;
import com.wavefront.agent.core.buffers.BuffersManager;
import com.wavefront.agent.core.senders.SenderTasksManager;
import com.wavefront.data.ReportableEntityType;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QueuesManager {
  private static Map<String, QueueInfo> queues = new ConcurrentHashMap<>();

  public static QueueInfo initQueue(ReportableEntityType entityType) {
    return initQueue(entityType, CENTRAL_TENANT_NAME);
  }

  public static QueueInfo initQueue(ReportableEntityType entityType, String tenant) {
    QueueInfo q = new Q(entityType, tenant);
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

  Q(ReportableEntityType entityType, String tenant) {
    this.name =
        entityType.name() + (tenant.equalsIgnoreCase(CENTRAL_TENANT_NAME) ? "" : "." + tenant);
    this.entityType = entityType;
    this.tenant = tenant;
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
}
