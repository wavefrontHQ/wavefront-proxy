package com.wavefront.agent.core.buffers;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.core.queues.QueueInfo;
import java.util.List;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;

public interface Buffer {
  void registerNewQueueInfo(QueueInfo key);

  void onMsgBatch(
      QueueInfo key, int idx, int batchSize, RecyclableRateLimiter rateLimiter, OnMsgFunction func);

  void shutdown();

  String getName();

  void sendPoints(String queue, List<String> strPoint) throws ActiveMQAddressFullException;
}
