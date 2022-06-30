package com.wavefront.agent.buffer;

import java.util.List;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;

public interface BufferBatch {

  void sendMsgs(QueueInfo key, List<String> strPoint) throws ActiveMQAddressFullException;
}
