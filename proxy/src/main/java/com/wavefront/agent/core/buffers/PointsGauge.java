package com.wavefront.agent.core.buffers;

import com.wavefront.agent.PushAgent;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.common.NamedThreadFactory;
import com.yammer.metrics.core.Gauge;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.server.ActiveMQServer;

public class PointsGauge extends Gauge<Long> {
  private static final Logger log = Logger.getLogger(PointsGauge.class.getCanonicalName());
  private static final ScheduledExecutorService executor =
      Executors.newScheduledThreadPool(2, new NamedThreadFactory("PointsGauge"));
  private Long pointsCount = 0L;
  private final QueueInfo queue;
  private final ActiveMQServer amq;

  public PointsGauge(QueueInfo queue, ActiveMQServer amq) {
    this.queue = queue;
    this.amq = amq;
    executor.scheduleAtFixedRate(() -> doCount(), 1, 1, TimeUnit.MINUTES);
  }

  @Override
  public Long value() {
    return pointsCount;
  }

  long doCount() {
    long count = 0;

    AddressControl address =
        (AddressControl)
            amq.getManagementService().getResource(ResourceNames.ADDRESS + queue.getName());

    try {
      for (String queueName : address.getQueueNames()) {
        QueueControl queueControl =
            (QueueControl) amq.getManagementService().getResource(ResourceNames.QUEUE + queueName);
        Map<String, Object>[] messages = queueControl.listMessages("");
        for (Map<String, Object> message : messages) {
          int p = (int) message.get("points");
          count += p;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    PushAgent.stats.info(
        "[buffer."
            + amq.getConfiguration().getName()
            + "."
            + queue.getName()
            + "] points: "
            + pointsCount);
    return pointsCount = count;
  }
}
