package com.wavefront.agent.core.buffers;

import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.common.NamedThreadFactory;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;

public class MemoryBuffer extends ActiveMQBuffer {
  private static final Logger logger = Logger.getLogger(MemoryBuffer.class.getCanonicalName());
  private static Map<String, LinkedTransferQueue<String>> midBuffers = new ConcurrentHashMap();
  private final ScheduledExecutorService executor;
  private BufferBatch nextBuffer;

  public MemoryBuffer(int level, String name, BufferConfig cfg) {
    super(level, name, false, cfg);
    executor =
        Executors.newScheduledThreadPool(
            Runtime.getRuntime().availableProcessors(),
            new NamedThreadFactory("memory-buffer-receiver"));
  }

  @Override
  public void shutdown() {
    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      logger.severe("Error during MemoryBuffer shutdown. " + e);
    }
    midBuffers.clear();
    super.shutdown();
  }

  public void sendMsg(QueueInfo key, String strPoint) {
    LinkedTransferQueue<String> midBuffer =
        midBuffers.computeIfAbsent(key.getName(), s -> new LinkedTransferQueue<>());
    midBuffer.add(strPoint);
  }

  @Override
  public void registerNewQueueInfo(QueueInfo queue) {
    // TODO
    //    int interval =
    // entityPropsFactoryMap.get(tenantName).get(entityType).getPushFlushInterval();

    super.registerNewQueueInfo(queue);
    for (int i = 0; i < queue.getNumberThreads(); i++) {
      executor.scheduleAtFixedRate(new sender(queue, nextBuffer), 1, 1, TimeUnit.SECONDS);
    }
  }

  public void flush(QueueInfo queue) {
    new sender(queue, nextBuffer).run();
  }

  public BufferBatch getNextBuffer() {
    return nextBuffer;
  }

  public void setNextBuffer(BufferBatch nextBuffer) {
    this.nextBuffer = nextBuffer;
  }

  private class sender implements Runnable {
    private final QueueInfo queue;
    private BufferBatch nextBuffer;

    private sender(QueueInfo queue, BufferBatch nextBuffer) {
      this.queue = queue;
      this.nextBuffer = nextBuffer;
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
              sendMsgs(queue, metrics);
            } catch (ActiveMQAddressFullException e) {
              logger.log(Level.SEVERE, "Memory Queue full");
              if (logger.isLoggable(Level.FINER)) {
                logger.log(Level.SEVERE, "", e);
              }
              try {
                nextBuffer.sendMsgs(queue, metrics);
              } catch (ActiveMQAddressFullException ex) {
                logger.log(
                    Level.SEVERE, "All Queues full, dropping " + metrics.size() + " points.");
                if (logger.isLoggable(Level.FINER)) {
                  logger.log(Level.SEVERE, "", e);
                }
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
