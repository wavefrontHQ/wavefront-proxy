package com.wavefront.agent.core.buffers;

import com.yammer.metrics.core.Gauge;
import java.util.function.Function;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;

public class MultiQueueGauge extends Gauge<Long> {
  private String queue;
  private EmbeddedActiveMQ amq;
  private Function<QueueControl, Long> func;

  public MultiQueueGauge(String queue, EmbeddedActiveMQ amq, Function<QueueControl, Long> func) {
    this.queue = queue;
    this.amq = amq;
    this.func = func;
  }

  @Override
  public Long value() {
    long res = 0;
    AddressControl address =
        (AddressControl)
            amq.getActiveMQServer()
                .getManagementService()
                .getResource(ResourceNames.ADDRESS + queue);

    try {
      for (String queueName : address.getQueueNames()) {
        QueueControl queueControl =
            (QueueControl)
                amq.getActiveMQServer()
                    .getManagementService()
                    .getResource(ResourceNames.QUEUE + queueName);
        res += func.apply(queueControl);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return res;
  }
}
