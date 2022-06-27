package com.wavefront.agent.buffer;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.yammer.metrics.core.Gauge;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;

public interface Buffer {
  void registerNewQueueInfo(QueueInfo key);

  void createBridge(String addr, QueueInfo queue, int level);

  void sendMsg(QueueInfo key, String strPoint) throws ActiveMQAddressFullException;

  void onMsgBatch(
      QueueInfo key, int batchSize, RecyclableRateLimiter rateLimiter, OnMsgFunction func);

  Gauge<Object> getMcGauge(QueueInfo key);

  void shutdown();
}
