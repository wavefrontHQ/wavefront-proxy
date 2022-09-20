package com.wavefront.agent.core.buffers;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.core.queues.QueueInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class BuffersManager {
  private static final Logger logger = Logger.getLogger(BuffersManager.class.getCanonicalName());

  private static final Map<String, Boolean> registeredQueues = new HashMap<>();
  private static MemoryBuffer memoryBuffer;
  private static DiskBuffer diskBuffer;
  private static Buffer external;

  public static void init(BuffersManagerConfig cfg) {
    memoryBuffer = new MemoryBuffer(0, "memory", cfg.memoryCfg);

    if (cfg.disk) {
      diskBuffer = new DiskBuffer(1, "disk", cfg.diskCfg);
      memoryBuffer.createBridge(diskBuffer);
    }

    if (cfg.external) {
      external = new SQSBuffer(cfg.sqsCfg);
      if (cfg.disk) {
        diskBuffer.setNextBuffer(external);
      } else {
        memoryBuffer.setNextBuffer(external);
      }
    }
  }

  public static void shutdown() {
    registeredQueues.clear();

    if (memoryBuffer != null) {
      memoryBuffer.shutdown();
      memoryBuffer = null;
    }
    if (diskBuffer != null) {
      diskBuffer.shutdown();
      diskBuffer = null;
    }
  }

  public static List<Buffer> registerNewQueueIfNeedIt(QueueInfo queue) {
    List<Buffer> buffers = new ArrayList<>();
    Boolean registered = registeredQueues.computeIfAbsent(queue.getName(), s -> false);
    if (!registered) { // is controlled by queue manager, but we do  it also here just in case.
      memoryBuffer.registerNewQueueInfo(queue);
      buffers.add(memoryBuffer);

      if (diskBuffer != null) {
        diskBuffer.registerNewQueueInfo(queue);
        buffers.add(diskBuffer);
      }

      if (external != null) {
        external.registerNewQueueInfo(queue);
        buffers.add(external);
      }

      registeredQueues.put(queue.getName(), true);
    }

    queue.getTenants().values().forEach(BuffersManager::registerNewQueueIfNeedIt);
    return buffers;
  }

  public static void sendMsg(QueueInfo queue, String strPoint) {
    memoryBuffer.sendPoint(queue, strPoint);
  }

  public static void onMsgBatch(
      QueueInfo handler,
      int idx,
      int batchSize,
      RecyclableRateLimiter rateLimiter,
      OnMsgFunction func) {
    memoryBuffer.onMsgBatch(handler, idx, batchSize, rateLimiter, func);
  }

  public static void truncateBacklog() {
    if (diskBuffer != null) {
      diskBuffer.truncate();
    }
  }
}
