package com.wavefront.agent.buffer;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.handlers.HandlerKey;
import com.yammer.metrics.core.Gauge;
import java.util.List;

public interface Buffer {
  void registerNewHandlerKey(HandlerKey key);

  void sendMsg(HandlerKey key, List<String> strPoints);

  void onMsg(HandlerKey key, OnMsgFunction func);

  void onMsgBatch(
      HandlerKey key, int batchSize, RecyclableRateLimiter rateLimiter, OnMsgFunction func);

  Gauge<Long> getMcGauge(HandlerKey key);

  void shutdown();
}
