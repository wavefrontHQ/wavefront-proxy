package com.wavefront.agent.core.buffers;

import com.google.common.util.concurrent.RecyclableRateLimiter;

public interface Buffer {
  void registerNewQueueInfo(com.wavefront.agent.core.queues.QueueInfo key);

  void createBridge(String addr, com.wavefront.agent.core.queues.QueueInfo queue, int level);

  void onMsgBatch(
      com.wavefront.agent.core.queues.QueueInfo key,
      int batchSize,
      RecyclableRateLimiter rateLimiter,
      OnMsgFunction func);

  void shutdown();
}
