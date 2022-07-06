package com.wavefront.agent.core.buffers;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.core.queues.QueueInfo;
import com.yammer.metrics.core.Gauge;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;
import org.jetbrains.annotations.TestOnly;

public class BuffersManager {
  private static final Logger logger = Logger.getLogger(BuffersManager.class.getCanonicalName());
  private static final Map<String, Boolean> registeredQueues = new HashMap<>();
  private static MemoryBuffer level_1;
  private static DiskBuffer level_2;
  //  private static Buffer level_3;
  private static ActiveMQAddressFullException ex;
  private static BuffersManagerConfig cfg;

  public static void init(BuffersManagerConfig cfg) {
    BuffersManager.cfg = cfg;

    registeredQueues.clear();

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
    level_1 = new MemoryBuffer(0, "memory", memCfg);

    if (cfg.l2) {
      BufferConfig dskCfg = new BufferConfig();
      dskCfg.buffer = cfg.buffer + "/disk";
      level_2 = new DiskBuffer(1, "disk", dskCfg);
      level_1.setNextBuffer(level_2);
    }
  }

  public static List<Buffer> registerNewQueueIfNeedIt(QueueInfo handler) {
    List<Buffer> buffers = new ArrayList<>();
    Boolean registered = registeredQueues.computeIfAbsent(handler.getName(), s -> false);
    if (!registered) { // is controlled by queue manager, but we do  it also here just in case.

      level_1.registerNewQueueInfo(handler);
      buffers.add(level_1);
      if (level_2 != null) {
        level_2.registerNewQueueInfo(handler);
        buffers.add(level_2);
        level_1.createBridge("disk", handler, 1);
        //        RatedBridge.createNewBridge(
        //            level_2,
        //            level_1,
        //            handler,
        //            entityPropertiesFactoryMap
        //                .get(CENTRAL_TENANT_NAME)
        //                .get(handler.getEntityType())
        //                .getRateLimit());
      }

      // TODO: move this to handler/queueInfo creation
      registeredQueues.put(handler.getName(), true);
    }
    return buffers;
  }

  public static void sendMsg(QueueInfo handler, String strPoint) {
    level_1.sendMsg(handler, strPoint);
  }

  public static void onMsgBatch(
      QueueInfo handler,
      int idx,
      int batchSize,
      RecyclableRateLimiter rateLimiter,
      OnMsgFunction func) {
    level_1.onMsgBatch(handler, idx, batchSize, rateLimiter, func);
  }

  public static void flush(QueueInfo queue) {
    level_1.flush(queue);
  }

  @TestOnly
  static Gauge<Object> l1_getSizeGauge(QueueInfo handler) {
    return level_1.getSizeGauge(handler);
  }

  @TestOnly
  static Gauge<Object> l2_getSizeGauge(QueueInfo handler) {
    return level_2.getSizeGauge(handler);
  }

  @TestOnly
  static ActiveMQBuffer getLeve1() {
    return level_1;
  }

  @TestOnly
  static Buffer getLeve2() {
    return level_2;
  }
}
