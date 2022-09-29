package com.wavefront.agent.core.buffers;

import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.data.EntityRateLimiter;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;

import java.util.List;

public interface Buffer {
  void registerNewQueueInfo(QueueInfo key);

  void onMsgBatch(
          QueueInfo key, int idx, int batchSize, EntityRateLimiter rateLimiter, OnMsgFunction func);

  void sendPoints(String queue, List<String> strPoint) throws ActiveMQAddressFullException;
}
