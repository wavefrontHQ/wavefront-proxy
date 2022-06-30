package com.wavefront.agent.buffer;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.yammer.metrics.core.Gauge;

public interface Buffer {
  void registerNewQueueInfo(QueueInfo key);

  void createBridge(String addr, QueueInfo queue, int level);

  void onMsgBatch(
      QueueInfo key, int batchSize, RecyclableRateLimiter rateLimiter, OnMsgFunction func);

  Gauge<Object> getMcGauge(QueueInfo key);

  void shutdown();
}
