package com.wavefront.agent.buffer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.buffer.activeMQ.BufferDisk;
import com.wavefront.agent.buffer.activeMQ.BufferMemory;
import com.wavefront.agent.handlers.HandlerKey;
import com.yammer.metrics.core.Gauge;
import java.util.List;
import java.util.logging.Logger;

public class BuffersManager {
  private static final Logger logger = Logger.getLogger(BuffersManager.class.getCanonicalName());

  private static Buffer level_1;
  private static SecondaryBuffer level_2;
  private static SecondaryBuffer level_3;

  public static void init(BuffersManagerConfig cfg) {
    if (level_1 != null) {
      level_1.shutdown();
    }
    if (level_2 != null) {
      level_2.shutdown();
    }

    level_1 = new BufferMemory(0, "memory", cfg.buffer + "/memory");
    if (cfg.l2) {
      level_2 = new BufferDisk(1, "disk", cfg.buffer + "/disk");
    }
  }

  public static void registerNewHandlerKey(HandlerKey handler) {
    level_1.registerNewHandlerKey(handler);
    if (level_2 != null) {
      level_2.registerNewHandlerKey(handler);
      level_2.createBridge(handler, 1);
    }
  }

  public static void sendMsg(HandlerKey handler, List<String> strPoints) {
    level_1.sendMsg(handler, strPoints);
  }

  @VisibleForTesting
  static Gauge<Long> l1GetMcGauge(HandlerKey handler) {
    return level_1.getMcGauge(handler);
  }

  @VisibleForTesting
  static Gauge<Long> l2GetMcGauge(HandlerKey handler) {
    return level_2.getMcGauge(handler);
  }

  public static void onMsgBatch(
      HandlerKey handler, int batchSize, RecyclableRateLimiter rateLimiter, OnMsgFunction func) {
    level_1.onMsgBatch(handler, batchSize, rateLimiter, func);
  }

  public static void onMsg(HandlerKey handler, OnMsgFunction func) {
    level_1.onMsg(handler, func);
  }

  @VisibleForTesting
  static Buffer getLeve2() {
    return level_2;
  }
}
