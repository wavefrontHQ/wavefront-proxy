package com.wavefront.agent.buffer;

import com.google.common.annotations.VisibleForTesting;
import com.yammer.metrics.core.Gauge;
import java.util.List;
import java.util.logging.Logger;

public class BuffersManager {
  private static final Logger logger = Logger.getLogger(BuffersManager.class.getCanonicalName());

  private static BufferActiveMQ level_1;
  private static Buffer level_2;
  private static Buffer level_3;

  public static void init(BuffersManagerConfig cfg) {
    level_1 = new BufferMemory(0, "memory", cfg.buffer + "/memory");
    if (cfg.l2) {
      level_2 = new BufferDisk(1, "disk", cfg.buffer + "/disk");
    }
  }

  public static void registerNewPort(String port) {
    level_1.registerNewPort(port);
    if (level_2 != null) {
      level_2.registerNewPort(port);
      level_1.createBridge(port, 1);
    }
  }

  public static void sendMsg(String port, List<String> strPoints) {
    level_1.sendMsg(port, strPoints);
  }

  @VisibleForTesting
  static Gauge<Long> l1GetMcGauge(String port) {
    return level_1.getMcGauge(port);
  }

  @VisibleForTesting
  static Gauge<Long> l2GetMcGauge(String port) {
    return level_2.getMcGauge(port);
  }

  public static void onMsgBatch(String port, int batchSize, OnMsgFunction func) {
    level_1.onMsgBatch(port, batchSize, func);
  }

  public static void onMsg(String port, OnMsgFunction func) {
    level_1.onMsg(port, func);
  }

  @VisibleForTesting
  static Buffer getLeve2() {
    return level_2;
  }
}
