package com.wavefront.agent.handlers;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.agent.data.TaskResult;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.common.logger.SharedRateLimitingLogger;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;

/**
 * Base class for all {@link SenderTask} implementations.
 *
 * @param <T> the type of input objects handled.
 */
abstract class AbstractSenderTask<T> implements SenderTask<T>, Runnable {
  private static final Logger logger =
      Logger.getLogger(AbstractSenderTask.class.getCanonicalName());

  /** Warn about exceeding the rate limit no more than once every 5 seconds */
  protected final Logger throttledLogger;

  final Object mutex = new Object();
  final ScheduledExecutorService scheduler;
  private final ExecutorService flushExecutor;

  final HandlerKey handlerKey;
  final int threadId;
  final EntityProperties properties;
  final RecyclableRateLimiter rateLimiter;

  final Counter attemptedCounter;
  final Counter blockedCounter;
  final Counter bufferFlushCounter;
  final Counter bufferCompletedFlushCounter;
  private final Histogram metricSize;

  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  final AtomicBoolean isBuffering = new AtomicBoolean(false);
  volatile boolean isSending = false;

  /**
   * Attempt to schedule drainBuffersToQueueTask no more than once every 100ms to reduce scheduler
   * overhead under memory pressure.
   */
  @SuppressWarnings("UnstableApiUsage")
  private final RateLimiter drainBuffersRateLimiter = RateLimiter.create(10);

  private static EmbeddedActiveMQ embeddedMen;
  private static EmbeddedActiveMQ embeddedDisk;
  private static ClientConsumer consumer;
  private static ClientSession session;

  static {
    try {
      //      System.out.println("-> " +
      // AbstractSenderTask.class.getClassLoader().getResource("broker_disk.xml").toString());
      //      embeddedDisk = new EmbeddedActiveMQ();
      //
      // embeddedDisk.setConfigResourcePath(AbstractSenderTask.class.getClassLoader().getResource("broker_disk.xml").toString());
      //      embeddedDisk.start();
      //
      //      embeddedMen = new EmbeddedActiveMQ();
      //
      // embeddedMen.setConfigResourcePath(AbstractSenderTask.class.getClassLoader().getResource("broker.xml").toString());
      //      embeddedMen.start();
      //
      //      ServerLocator serverLocator = ActiveMQClient.createServerLocator("vm://0");
      //      ClientSessionFactory factory = serverLocator.createSessionFactory();
      //      session = factory.createSession(true,true);
      //
      //      consumer = session.createConsumer("memory::points");

      //      MBeanInfo obj = embedded.getActiveMQServer().getMBeanServer().getMBeanInfo();
      //              System.out.println(obj);

      //      ObjectName nameMen = new
      // ObjectName("org.apache.activemq.artemis:broker=\"memory\",component=addresses,address=\"memoryBuffer\"");
      //      ObjectName nameDisk = new
      // ObjectName("org.apache.activemq.artemis:broker=\"disk\",component=addresses,address=\"diskBuffer\"");
      //      Metrics.newGauge(new MetricName("buffer.memory", "", "MessageCount"), new
      // Gauge<Integer>() {
      //        @Override
      //        public Integer value() {
      //          Long mc = null;
      //          try {
      //            mc = (Long)
      // embeddedMen.getActiveMQServer().getMBeanServer().getAttribute(nameMen, "MessageCount");
      //          } catch (Exception e) {
      //            e.printStackTrace();
      //            return 0;
      //          }
      //          return mc.intValue(); //datum.size();
      //        }
      //      });
      //
      //      Metrics.newGauge(new MetricName("buffer.disk", "", "MessageCount"), new
      // Gauge<Integer>() {
      //        @Override
      //        public Integer value() {
      //          Long mc = null;
      //          try {
      //            mc = (Long)
      // embeddedDisk.getActiveMQServer().getMBeanServer().getAttribute(nameDisk, "MessageCount");
      //          } catch (Exception e) {
      //            e.printStackTrace();
      //            return 0;
      //          }
      //          return mc.intValue(); //datum.size();
      //        }
      //      });

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * Base constructor.
   *
   * @param handlerKey pipeline handler key that dictates the data processing flow.
   * @param threadId thread number
   * @param properties runtime properties container
   * @param scheduler executor service for running this task
   */
  AbstractSenderTask(
      HandlerKey handlerKey,
      int threadId,
      EntityProperties properties,
      ScheduledExecutorService scheduler) {
    this.handlerKey = handlerKey;
    this.threadId = threadId;
    this.properties = properties;
    this.rateLimiter = properties.getRateLimiter();
    this.scheduler = scheduler;
    this.throttledLogger = new SharedRateLimitingLogger(logger, "rateLimit-" + handlerKey, 0.2);
    this.flushExecutor =
        new ThreadPoolExecutor(
            1,
            1,
            60L,
            TimeUnit.MINUTES,
            new SynchronousQueue<>(),
            new NamedThreadFactory("flush-" + handlerKey.toString() + "-" + threadId));

    this.attemptedCounter = Metrics.newCounter(new MetricName(handlerKey.toString(), "", "sent"));
    this.blockedCounter = Metrics.newCounter(new MetricName(handlerKey.toString(), "", "blocked"));
    this.bufferFlushCounter =
        Metrics.newCounter(
            new TaggedMetricName("buffer", "flush-count", "port", handlerKey.getHandle()));
    this.bufferCompletedFlushCounter =
        Metrics.newCounter(
            new TaggedMetricName(
                "buffer", "completed-flush-count", "port", handlerKey.getHandle()));

    this.metricSize =
        Metrics.newHistogram(
            new MetricName(handlerKey.toString() + "." + threadId, "", "metric_length"));
    Metrics.newGauge(
        new MetricName(handlerKey.toString() + "." + threadId, "", "size"),
        new Gauge<Integer>() {
          @Override
          public Integer value() {
            return 0; // datum.size();
          }
        });

    //    try {
    //      ServerLocator serverLocator = ActiveMQClient.createServerLocator("vm://0");
    //      ClientSessionFactory factory = serverLocator.createSessionFactory();
    //      session = factory.createSession();
    //      session.start();
    //      consumer = session.createConsumer("example");

    //      ClientMessage message = session.createMessage(true);
    //      message.writeBodyBufferString("Hello");
    //      producer.send(message);
    //
    //      ClientMessage msgReceived = consumer.receive(1);
    //      System.out.println("message = " + msgReceived.getReadOnlyBodyBuffer().readString());

    //    } catch (Exception e) {
    //      e.printStackTrace();
    //      System.exit(-1);
    //    }
  }

  abstract TaskResult processSingleBatch(List<T> batch);

  @Override
  public void run() {
    if (!isRunning.get()) return;
    long nextRunMillis = properties.getPushFlushInterval();
    isSending = true;
    try {
      session.start();
      List<T> current = createBatch();
      int currentBatchSize = getDataSize(current);
      if (currentBatchSize == 0) return;
      TaskResult result = processSingleBatch(current);
      this.attemptedCounter.inc(currentBatchSize);
      switch (result) {
        case DELIVERED:
          session.commit();
          break;
        case PERSISTED:
        case PERSISTED_RETRY:
        case RETRY_LATER:
        default:
          session.rollback(true);
      }
    } catch (Throwable t) {
      logger.log(Level.SEVERE, "Unexpected error in flush loop", t);
    } finally {
      isSending = false;
      if (isRunning.get()) {
        scheduler.schedule(this, nextRunMillis, TimeUnit.MILLISECONDS);
      }
    }
  }

  @Override
  public void start() {
    if (isRunning.compareAndSet(false, true)) {
      this.scheduler.schedule(this, properties.getPushFlushInterval(), TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void stop() {
    isRunning.set(false);
    flushExecutor.shutdown();
  }

  @Override
  public void add(T metricString) {
    metricSize.update(metricString.toString().length());
    //    try {
    //      ClientMessage message = session.createMessage(true);
    //      message.writeBodyBufferString(metricString.toString());
    //      producer.send(message);
    //    } catch (Exception e) {
    //      e.printStackTrace();
    //      System.exit(-1);
    //    }
  }

  protected List<T> createBatch() {
    int blockSize = Math.min(properties.getItemsPerBatch(), (int) rateLimiter.getRate());
    List<T> current = new ArrayList<>(blockSize);
    boolean done = false;
    long start = System.currentTimeMillis();
    try {
      while (!done
          && (current.size() < blockSize)
          && ((System.currentTimeMillis() - start) < 1000)) {
        ClientMessage msgReceived = consumer.receive(1);
        if (msgReceived != null) {
          System.out.println("--- q -> msg");
          current.add((T) msgReceived.getReadOnlyBodyBuffer().readString());
        } else {
          done = true;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
    return current != null ? current : new ArrayList<T>();
  }

  protected void undoBatch(List<T> batch) {
    //    synchronized (mutex) {
    //      datum.addAll(0, batch);
    //    }
  }

  private final Runnable drainBuffersToQueueTask =
      new Runnable() {
        @Override
        public void run() {
          //      if (datum.size() > properties.getMemoryBufferLimit()) {
          //        // there are going to be too many points to be able to flush w/o the agent
          // blowing up
          //        // drain the leftovers straight to the retry queue (i.e. to disk)
          //        // don't let anyone add any more to points while we're draining it.
          //        logger.warning("[" + handlerKey.getHandle() + " thread " + threadId +
          //            "]: WF-3 Too many pending " + handlerKey.getEntityType() + " (" +
          // datum.size() +
          //            "), block size: " + properties.getItemsPerBatch() + ". flushing to retry
          // queue");
          //        drainBuffersToQueue(QueueingReason.BUFFER_SIZE);
          //        logger.info("[" + handlerKey.getHandle() + " thread " + threadId +
          //            "]: flushing to retry queue complete. Pending " + handlerKey.getEntityType()
          // +
          //            ": " + datum.size());
          //      }
        }
      };

  abstract void flushSingleBatch(List<T> batch, @Nullable QueueingReason reason);

  public void drainBuffersToQueue(@Nullable QueueingReason reason) {
    //    if (isBuffering.compareAndSet(false, true)) {
    //      bufferFlushCounter.inc();
    //      try {
    //        int lastBatchSize = Integer.MIN_VALUE;
    //        // roughly limit number of items to flush to the the current buffer size (+1 blockSize
    // max)
    //        // if too many points arrive at the proxy while it's draining,
    //        // they will be taken care of in the next run
    //        int toFlush = datum.size();
    //        while (toFlush > 0) {
    //          List<T> batch = createBatch();
    //          int batchSize = batch.size();
    //          if (batchSize > 0) {
    //            flushSingleBatch(batch, reason);
    //            // update the counters as if this was a failed call to the API
    //            this.attemptedCounter.inc(batchSize);
    //            toFlush -= batchSize;
    //            // stop draining buffers if the batch is smaller than the previous one
    //            if (batchSize < lastBatchSize) {
    //              break;
    //            }
    //            lastBatchSize = batchSize;
    //          } else {
    //            break;
    //          }
    //        }
    //      } finally {
    //        isBuffering.set(false);
    //        bufferCompletedFlushCounter.inc();
    //      }
    //    }
  }

  @Override
  public long getTaskRelativeScore() {
    return 0; // datum.size() + (isBuffering.get() ? properties.getMemoryBufferLimit() :
    //        (isSending ? properties.getItemsPerBatch() / 2 : 0));
  }
}
