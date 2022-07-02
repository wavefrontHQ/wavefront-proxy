package com.wavefront.agent.core.buffers;

import java.util.List;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;

public interface BufferBatch {
  void sendMsgs(com.wavefront.agent.core.queues.QueueInfo key, List<String> strPoint)
      throws ActiveMQAddressFullException;
}
