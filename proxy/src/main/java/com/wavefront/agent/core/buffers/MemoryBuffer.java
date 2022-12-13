package com.wavefront.agent.core.buffers;

import static org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy.FAIL;

import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.common.NamedThreadFactory;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.*;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryBuffer extends ActiveMQBuffer {
  private static final Logger log = LoggerFactory.getLogger(MemoryBuffer.class.getCanonicalName());
  private static final Logger slowLog = log;
  //      new MessageDedupingLogger(LoggerFactory.getLogger(MemoryBuffer.class.getCanonicalName()),
  // 1000, 1);
  private static final Logger droppedPointsLogger = LoggerFactory.getLogger("RawDroppedPoints");

  private static final Map<String, LinkedTransferQueue<String>> midBuffers =
      new ConcurrentHashMap<>();
  private final ScheduledExecutorService executor;
  private final MemoryBufferConfig cfg;

  public MemoryBuffer(int level, String name, MemoryBufferConfig cfg) {
    super(level, name, false, null, cfg.maxMemory);
    this.cfg = cfg;
    executor =
        Executors.newScheduledThreadPool(
            Runtime.getRuntime().availableProcessors(),
            new NamedThreadFactory("memory-buffer-receiver"));
  }

  public String getName() {
    return "Memory";
  }

  @Override
  public int getPriority() {
    return Thread.MAX_PRIORITY;
  }

  public void shutdown() {
    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      log.error("Error during MemoryBuffer shutdown. " + e);
    }

    // TODO: implement dump to external queue
    if (this.nextBuffer instanceof DiskBuffer) {
      if (((DiskBuffer) nextBuffer).isFull()) {
        return;
      }
    }

    int counter = 0;
    try {
      Object[] queues = activeMQServer.getManagementService().getResources(QueueControl.class);
      for (Object obj : queues) {
        QueueControl queue = (QueueControl) obj;
        int c = queue.expireMessages("");
        counter += c;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (counter != 0) {
      log.info("'" + counter + "' points sent to the buffer disk");
    }

    super.shutdown();
  }

  public void sendPoint(QueueInfo queue, String strPoint) {
    QueueStats.get(queue.getName()).msgLength.update(strPoint.length());
    LinkedTransferQueue<String> midBuffer =
        midBuffers.computeIfAbsent(queue.getName(), s -> new LinkedTransferQueue<>());
    midBuffer.add(strPoint);
  }

  @Override
  public void registerNewQueueInfo(QueueInfo queue) {
    super.registerNewQueueInfo(queue);
    for (int i = 0; i < queue.getNumberThreads(); i++) {
      executor.scheduleAtFixedRate(new sender(queue), 1, 1, TimeUnit.SECONDS);
    }
  }

  protected void createBridge(DiskBuffer diskBuffer) {
    setNextBuffer(diskBuffer);
    activeMQServer.registerBrokerPlugin(new Bridge(this, diskBuffer));
    enableBridge();
  }

  protected void enableBridge() {
    log.info("bridge enabled");
    AddressSettings addressSetting = activeMQServer.getAddressSettingsRepository().getDefault();
    addressSetting.setMaxExpiryDelay(cfg.msgExpirationTime);
    addressSetting.setMaxDeliveryAttempts(cfg.msgRetry);
    addressSetting.setMaxSizeBytes(cfg.maxMemory);
    addressSetting.setAddressFullMessagePolicy(FAIL);
    activeMQServer.getAddressSettingsRepository().setDefault(addressSetting);
  }

  protected void disableBridge() {
    log.info("bridge disabled");
    AddressSettings addressSetting = activeMQServer.getAddressSettingsRepository().getDefault();
    addressSetting.setMaxExpiryDelay(-1L);
    addressSetting.setMaxDeliveryAttempts(-1);
    addressSetting.setMaxSizeBytes(cfg.maxMemory);
    addressSetting.setAddressFullMessagePolicy(FAIL);
    activeMQServer.getAddressSettingsRepository().setDefault(addressSetting);
  }

  protected void flush(QueueInfo queue) {
    new sender(queue).run();
  }

  private class sender implements Runnable {
    private final QueueInfo queue;

    private sender(QueueInfo queue) {
      this.queue = queue;
    }

    @Override
    public void run() {
      LinkedTransferQueue<String> midBuffer = midBuffers.get(queue.getName());
      if ((midBuffer != null) && (midBuffer.size() != 0)) {
        boolean done = false;
        while (!done) {
          ArrayList<String> metrics = new ArrayList<>();
          if (midBuffer.drainTo(metrics, queue.getMaxItemsPerMessage()) != 0) {
            try {
              sendPoints(queue.getName(), metrics);
            } catch (ActiveMQAddressFullException e) {
              slowLog.error("All Queues full, dropping " + metrics.size() + " points.");
              if (slowLog.isDebugEnabled()) {
                slowLog.error("", e);
              }
              QueueStats.get(queue.getName()).dropped.inc(metrics.size());
              if (droppedPointsLogger.isInfoEnabled()) {
                metrics.forEach(point -> droppedPointsLogger.info(point, queue.getEntityType()));
              }
            }
          } else {
            done = true;
          }
        }
      }
    }
  }
}
