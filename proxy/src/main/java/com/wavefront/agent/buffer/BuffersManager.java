package com.wavefront.agent.buffer;

import static com.wavefront.agent.api.APIContainer.CENTRAL_TENANT_NAME;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.buffer.activeMQ.BufferActiveMQ;
import com.wavefront.agent.buffer.activeMQ.BufferDisk;
import com.wavefront.agent.buffer.activeMQ.BufferMemory;
import com.wavefront.agent.data.EntityPropertiesFactory;
import com.wavefront.agent.handlers.SenderTaskFactory;
import com.yammer.metrics.core.Gauge;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private static SenderTaskFactory senderTaskFactory;
  private static Map<String, EntityPropertiesFactory> entityPropertiesFactoryMap;
  private static final Map<String, Boolean> registeredQueues = new HashMap<>();

  public static void init(
      BuffersManagerConfig cfg,
      SenderTaskFactory senderTaskFactory,
      Map<String, EntityPropertiesFactory> entityPropertiesFactoryMap) {
    BuffersManager.cfg = cfg;
    BuffersManager.senderTaskFactory = senderTaskFactory;
    BuffersManager.entityPropertiesFactoryMap = entityPropertiesFactoryMap;

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

  public static void registerNewQueueIfNeedIt(QueueInfo handler) {
    Boolean registered = registeredQueues.computeIfAbsent(handler.getQueue(), s -> false);
    if (!registered) {
      level_1.registerNewQueueInfo(handler);
      if (level_2 != null) {
        level_2.registerNewQueueInfo(handler);
        level_1.createBridge("disk", handler, 1);
        RatedBridge.createNewBridge(
            level_2,
            level_1,
            handler,
            entityPropertiesFactoryMap
                .get(CENTRAL_TENANT_NAME)
                .get(handler.getEntityType())
                .getRateLimit());
      }

      senderTaskFactory.createSenderTasks(handler);
      registeredQueues.put(handler.getQueue(), true);
    }
  }

  public static void sendMsg(QueueInfo handler, List<String> strPoints) {
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
      QueueInfo handler, int batchSize, RecyclableRateLimiter rateLimiter, OnMsgFunction func) {
    level_1.onMsgBatch(handler, batchSize, rateLimiter, func);
  }

  @TestOnly
  static Gauge<Object> l1GetMcGauge(QueueInfo handler) {
    return level_1.getMcGauge(handler);
  }

  @TestOnly
  static Gauge<Object> l2GetMcGauge(QueueInfo handler) {
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
