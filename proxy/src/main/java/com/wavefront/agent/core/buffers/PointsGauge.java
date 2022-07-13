package com.wavefront.agent.core.buffers;

import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.common.NamedThreadFactory;
import com.yammer.metrics.core.Gauge;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.apache.activemq.artemis.api.core.client.*;

public class PointsGauge extends Gauge<Long> {
  private static final Logger log = Logger.getLogger(PointsGauge.class.getCanonicalName());
  private static final ScheduledExecutorService excutor =
      Executors.newScheduledThreadPool(2, new NamedThreadFactory("PointsGauge"));
  private Long pointsCount = 0L;
  private final QueueInfo queue;
  private final int serverID;

  public PointsGauge(QueueInfo queue, int serverID) {
    this.queue = queue;
    this.serverID = serverID;
    excutor.scheduleAtFixedRate(() -> doCount(), 1, 1, TimeUnit.MINUTES);
  }

  @Override
  public Long value() {
    return pointsCount;
  }

  private void doCount() {
    long count = 0;
    try {
      ServerLocator serverLocator = ActiveMQClient.createServerLocator("vm://" + serverID);
      ClientSessionFactory factory = serverLocator.createSessionFactory();
      for (int q_idx = 0; q_idx < queue.getNumberThreads(); q_idx++) {
        ClientSession session = factory.createSession(true, true);
        ClientConsumer client = session.createConsumer(queue.getName() + "." + q_idx, true);
        boolean done = false;
        while (!done) {
          ClientMessage msg = client.receive(100);
          if (msg != null) {
            count += msg.getIntProperty("points");
          } else {
            done = true;
          }
        }
        client.close();
        session.close();
      }
    } catch (Throwable e) {
      log.severe("Error counting disk queue messages." + e.getMessage());
    }
    pointsCount = count;
  }
}
