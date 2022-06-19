package com.wavefront.agent.buffer;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.buffer.activeMQ.BufferActiveMQ;
import com.wavefront.agent.buffer.activeMQ.BufferDisk;
import com.wavefront.agent.buffer.activeMQ.BufferMemory;
import com.wavefront.agent.handlers.HandlerKey;
import com.yammer.metrics.core.Gauge;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;
import org.jetbrains.annotations.TestOnly;

public class BuffersManager {
  private static final Logger logger = Logger.getLogger(BuffersManager.class.getCanonicalName());

  private static Buffer level_1;
  private static Buffer level_2;
  private static Buffer level_3;
  private static ActiveMQAddressFullException ex;
  private static BuffersManagerConfig cfg;

  public static void init(BuffersManagerConfig cfg) {
    BuffersManager.cfg = cfg;
    if (level_1 != null) {
      level_1.shutdown();
      level_1 = null;
    }
    if (level_2 != null) {
      level_2.shutdown();
      level_2 = null;
    }

    BufferConfig memCfg = new BufferConfig();
    memCfg.buffer = cfg.buffer + "/memory";
    memCfg.msgExpirationTime = cfg.msgExpirationTime;
    memCfg.msgRetry = cfg.msgRetry;
    level_1 = new BufferMemory(0, "memory", memCfg);

    if (cfg.l2) {
      BufferConfig dskCfg = new BufferConfig();
      dskCfg.buffer = cfg.buffer + "/disk";
      level_2 = new BufferDisk(1, "disk", dskCfg);
    }
  }

  public static void registerNewHandlerKey(HandlerKey handler) {
    level_1.registerNewHandlerKey(handler);
    if (level_2 != null) {
      level_2.registerNewHandlerKey(handler);
      level_1.createBridge("disk", handler, 1);
    }
  }

  public static void sendMsg(HandlerKey handler, List<String> strPoints) {
    try {
      level_1.sendMsg(handler, strPoints);
    } catch (ActiveMQAddressFullException e) {
      if (level_2 != null) {
        try {
          level_2.sendMsg(handler, strPoints);
        } catch (ActiveMQAddressFullException ex) {
          if (level_3 != null) {
            try {
              level_3.sendMsg(handler, strPoints);
            } catch (ActiveMQAddressFullException exx) {
              logger.log(Level.SEVERE, exx.getMessage(), exx);
            }
          } else {
            logger.log(Level.SEVERE, ex.getMessage(), ex);
          }
        }
      } else {
        logger.log(Level.SEVERE, e.getMessage(), e);
      }
    }
  }

  public static void onMsgBatch(
      HandlerKey handler, int batchSize, RecyclableRateLimiter rateLimiter, OnMsgFunction func) {
    level_1.onMsgBatch(handler, batchSize, rateLimiter, func);
  }

  @TestOnly
  static Gauge<Long> l1GetMcGauge(HandlerKey handler) {
    return level_1.getMcGauge(handler);
  }

  @TestOnly
  static Gauge<Long> l2GetMcGauge(HandlerKey handler) {
    return level_2.getMcGauge(handler);
  }

  @TestOnly
  static BufferActiveMQ getLeve1() {
    return (BufferActiveMQ) level_1;
  }

  @TestOnly
  static Buffer getLeve2() {
    return level_2;
  }
}
