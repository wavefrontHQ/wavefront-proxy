package com.wavefront.agent.core.queues;

import static com.wavefront.agent.api.APIContainer.CENTRAL_TENANT_NAME;

import com.wavefront.agent.ProxyConfig;
import com.wavefront.agent.core.buffers.Buffer;
import com.wavefront.agent.core.buffers.BuffersManager;
import com.wavefront.agent.core.senders.SenderTasksManager;
import com.wavefront.agent.data.EntityPropertiesFactory;
import com.wavefront.data.ReportableEntityType;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QueuesManagerDefault implements QueuesManager {
  private Map<String, QueueInfo> queues = new ConcurrentHashMap<>();
  private Map<String, EntityPropertiesFactory> entityProperties;
  private ProxyConfig cfg;

  public QueuesManagerDefault(
      Map<String, EntityPropertiesFactory> entityPropertiesFactoryMap, ProxyConfig cfg) {
    this.entityProperties = entityPropertiesFactoryMap;
    this.cfg = cfg;
  }

  public QueueInfo initQueue(ReportableEntityType entityType) {
    Queue queue = initQueue(entityType, CENTRAL_TENANT_NAME);
    cfg.getMulticastingTenantList()
        .keySet()
        .forEach(
            tenat -> {
              queue.addTenant(tenat, initQueue(entityType, tenat));
            });
    return queue;
  }

  private Queue initQueue(ReportableEntityType entityType, String tenant) {
    Queue queue =
        new Queue(
            entityType, tenant, entityProperties.get(tenant).get(entityType).getFlushThreads());
    queues.computeIfAbsent(
        queue.getName(),
        s -> {
          setupQueue(queue);
          return queue;
        });
    return queue;
  }

  private static void setupQueue(QueueInfo q) {
    List<Buffer> buffers = BuffersManager.registerNewQueueIfNeedIt(q);
    buffers.forEach(buffer -> SenderTasksManager.createSenderTasks(q, buffer, 1));
  }
}
