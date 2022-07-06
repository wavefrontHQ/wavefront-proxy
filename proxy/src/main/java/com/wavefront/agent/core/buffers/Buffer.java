package com.wavefront.agent.core.buffers;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.core.queues.QueueInfo;

public interface Buffer {
  void registerNewQueueInfo(QueueInfo key);

  void createBridge(String addr, QueueInfo queue, int level);

  void onMsgBatch(
      QueueInfo key, int idx, int batchSize, RecyclableRateLimiter rateLimiter, OnMsgFunction func);

  void shutdown();
}
