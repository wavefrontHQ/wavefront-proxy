package com.wavefront.agent.buffer;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.handlers.HandlerKey;
import com.yammer.metrics.core.Gauge;
import java.util.List;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;

public interface Buffer {
  void registerNewHandlerKey(HandlerKey key);

  void createBridge(String addr, HandlerKey queue, int level);

  void sendMsg(HandlerKey key, List<String> strPoints) throws ActiveMQAddressFullException;

  void onMsgBatch(
      HandlerKey key, int batchSize, RecyclableRateLimiter rateLimiter, OnMsgFunction func);

  Gauge<Long> getMcGauge(HandlerKey key);

  void shutdown();
}
