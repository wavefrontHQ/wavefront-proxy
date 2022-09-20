package com.wavefront.agent.core.buffers;

import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.common.logger.MessageDedupingLogger;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;
import org.apache.activemq.artemis.api.core.management.QueueControl;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;

public class MemoryBuffer extends ActiveMQBuffer {
  private static final Logger log = Logger.getLogger(MemoryBuffer.class.getCanonicalName());
  private static final Logger slowLog = new MessageDedupingLogger(
      Logger.getLogger(MemoryBuffer.class.getCanonicalName()), 1000, 1);
  private static final Logger droppedPointsLogger = Logger.getLogger("RawDroppedPoints");

  private static final Map<String, LinkedTransferQueue<String>> midBuffers = new ConcurrentHashMap<>();
  private final ScheduledExecutorService executor;
  private final MemoryBufferConfig cfg;

  public MemoryBuffer(int level, String name, MemoryBufferConfig cfg) {
    super(level, name, false, null, cfg.maxMemory);
    this.cfg = cfg;
    executor = Executors.newScheduledThreadPool(
        Runtime.getRuntime().availableProcessors(),
        new NamedThreadFactory("memory-buffer-receiver"));
  }

  public void shutdown() {
    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      log.severe("Error during MemoryBuffer shutdown. " + e);
    }

    // TODO: implement dump to external queue
    if (this.nextBuffer instanceof DiskBuffer) {
      if (((DiskBuffer) nextBuffer).isFull()) {
        return;
      }
    }

    int counter = 0;
    try {
      Object[] queues = amq.getActiveMQServer().getManagementService().getResources(QueueControl.class);
      for (Object obj : queues) {
        QueueControl queue = (QueueControl) obj;
        int c = queue.expireMessages("");
        System.out.println("-> queue: " + queue.getName() + " - " + c);
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
    LinkedTransferQueue<String> midBuffer = midBuffers.computeIfAbsent(queue.getName(),
        s -> new LinkedTransferQueue<>());
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
    amq.getActiveMQServer().registerBrokerPlugin(new Bridge(this, diskBuffer));
    enableBridge();
  }

  protected void enableBridge() {
    log.info("bridge enabled");
    AddressSettings addressSetting = amq.getActiveMQServer().getAddressSettingsRepository().getDefault();
    addressSetting.setMaxExpiryDelay(cfg.msgExpirationTime);
    addressSetting.setMaxDeliveryAttempts(cfg.msgRetry);
    amq.getActiveMQServer().getAddressSettingsRepository().setDefault(addressSetting);
  }

  protected void disableBridge() {
    log.info("bridge disabled");
    AddressSettings addressSetting = amq.getActiveMQServer().getAddressSettingsRepository().getDefault();
    addressSetting.setMaxExpiryDelay(-1L);
    addressSetting.setMaxDeliveryAttempts(-1);
    amq.getActiveMQServer().getAddressSettingsRepository().setDefault(addressSetting);
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
          if (midBuffer.drainTo(metrics, 100) != 0) {
            try {
              sendPoints(queue.getName(), metrics);
            } catch (ActiveMQAddressFullException e) {
              slowLog.log(Level.SEVERE, "All Queues full, dropping " + metrics.size() + " points.");
              if (slowLog.isLoggable(Level.FINER)) {
                slowLog.log(Level.SEVERE, "", e);
              }
              QueueStats.get(queue.getName()).dropped.inc(metrics.size());
              if (droppedPointsLogger.isLoggable(Level.INFO)) {
                metrics.forEach(
                    point -> droppedPointsLogger.log(Level.INFO, point, queue.getEntityType()));
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
