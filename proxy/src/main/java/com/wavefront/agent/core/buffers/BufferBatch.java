package com.wavefront.agent.core.buffers;

import com.wavefront.agent.core.queues.QueueInfo;
import java.util.List;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;

public interface BufferBatch {
  void sendMsgs(QueueInfo key, List<String> strPoint) throws ActiveMQAddressFullException;
}
