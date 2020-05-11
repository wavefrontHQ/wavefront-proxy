package com.wavefront.agent.handlers;

import com.google.common.util.concurrent.RateLimiter;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.agent.data.TaskResult;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.common.SharedRateLimitingLogger;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Base class for all {@link SenderTask} implementations.
 *
 * @author vasily@wavefront.com
 *
 * @param <T> the type of input objects handled.
 */
abstract class AbstractSenderTask<T> implements SenderTask<T>, Runnable {
  private static final Logger logger = Logger.getLogger(AbstractSenderTask.class.getCanonicalName());

  /**
   * Warn about exceeding the rate limit no more than once every 5 seconds
   */
  protected final Logger throttledLogger;

  List<T> datum = new ArrayList<>();
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

  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  final AtomicBoolean isBuffering = new AtomicBoolean(false);
  volatile boolean isSending = false;

  /**
   * Attempt to schedule drainBuffersToQueueTask no more than once every 100ms to reduce
   * scheduler overhead under memory pressure.
   */
  @SuppressWarnings("UnstableApiUsage")
  private final RateLimiter drainBuffersRateLimiter = RateLimiter.create(10);

  /**
   * Base constructor.
   *
   * @param handlerKey pipeline handler key that dictates the data processing flow.
   * @param threadId   thread number
   * @param properties runtime properties container
   * @param scheduler  executor service for running this task
   */
  AbstractSenderTask(HandlerKey handlerKey, int threadId, EntityProperties properties,
                     ScheduledExecutorService scheduler) {
    this.handlerKey = handlerKey;
    this.threadId = threadId;
    this.properties = properties;
    this.rateLimiter = properties.getRateLimiter();
    this.scheduler = scheduler;
    this.throttledLogger = new SharedRateLimitingLogger(logger, "rateLimit-" + handlerKey, 0.2);
    this.flushExecutor = new ThreadPoolExecutor(1, 1, 60L, TimeUnit.MINUTES,
        new SynchronousQueue<>(), new NamedThreadFactory("flush-" + handlerKey.toString() +
        "-" + threadId));

    this.attemptedCounter = Metrics.newCounter(new MetricName(handlerKey.toString(), "", "sent"));
    this.blockedCounter = Metrics.newCounter(new MetricName(handlerKey.toString(), "", "blocked"));
    this.bufferFlushCounter = Metrics.newCounter(new TaggedMetricName("buffer", "flush-count",
        "port", handlerKey.getHandle()));
    this.bufferCompletedFlushCounter = Metrics.newCounter(new TaggedMetricName("buffer",
        "completed-flush-count", "port", handlerKey.getHandle()));
  }

  abstract TaskResult processSingleBatch(List<T> batch);

  @Override
  public void run() {
    if (!isRunning.get()) return;
    long nextRunMillis = properties.getPushFlushInterval();
    isSending = true;
    try {
      List<T> current = createBatch();
      int currentBatchSize = current.size();
      if (currentBatchSize == 0) return;
      if (rateLimiter == null || rateLimiter.tryAcquire(currentBatchSize)) {
        TaskResult result = processSingleBatch(current);
        this.attemptedCounter.inc(currentBatchSize);
        switch (result) {
          case DELIVERED:
            break;
          case PERSISTED:
          case PERSISTED_RETRY:
            if (rateLimiter != null) rateLimiter.recyclePermits(currentBatchSize);
            break;
          case RETRY_LATER:
            undoBatch(current);
            if (rateLimiter != null) rateLimiter.recyclePermits(currentBatchSize);
          default:
        }
      } else {
        // if proxy rate limit exceeded, try again in 1/4..1/2 of flush interval
        // to introduce some degree of fairness.
        nextRunMillis = nextRunMillis / 4 + (int) (Math.random() * nextRunMillis / 4);
        final long willRetryIn = nextRunMillis;
        throttledLogger.log(Level.INFO, () -> "[" + handlerKey.getHandle() + " thread " + threadId +
              "]: WF-4 Proxy rate limiter active (pending " + handlerKey.getEntityType() + ": " +
              datum.size() + "), will retry in " + willRetryIn + "ms");
        undoBatch(current);
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
    synchronized (mutex) {
      this.datum.add(metricString);
    }
    //noinspection UnstableApiUsage
    if (datum.size() >= properties.getMemoryBufferLimit() && !isBuffering.get() &&
        drainBuffersRateLimiter.tryAcquire()) {
      try {
        flushExecutor.submit(drainBuffersToQueueTask);
      } catch (RejectedExecutionException e) {
        // ignore - another task is already being executed
      }
    }
  }

  protected List<T> createBatch() {
    List<T> current;
    int blockSize;
    synchronized (mutex) {
      blockSize = Math.min(datum.size(), Math.min(properties.getItemsPerBatch(),
          (int)rateLimiter.getRate()));
      current = datum.subList(0, blockSize);
      datum = new ArrayList<>(datum.subList(blockSize, datum.size()));
    }
    logger.fine("[" + handlerKey.getHandle() + "] (DETAILED): sending " + current.size() +
        " valid " + handlerKey.getEntityType() + "; in memory: " + this.datum.size() +
        "; total attempted: " + this.attemptedCounter.count() +
        "; total blocked: " + this.blockedCounter.count());
    return current;
  }

  protected void undoBatch(List<T> batch) {
    synchronized (mutex) {
      datum.addAll(0, batch);
    }
  }

  private final Runnable drainBuffersToQueueTask = new Runnable() {
    @Override
    public void run() {
      if (datum.size() > properties.getMemoryBufferLimit()) {
        // there are going to be too many points to be able to flush w/o the agent blowing up
        // drain the leftovers straight to the retry queue (i.e. to disk)
        // don't let anyone add any more to points while we're draining it.
        logger.warning("[" + handlerKey.getHandle() + " thread " + threadId +
            "]: WF-3 Too many pending " + handlerKey.getEntityType() + " (" + datum.size() +
            "), block size: " + properties.getItemsPerBatch() + ". flushing to retry queue");
        drainBuffersToQueue(QueueingReason.BUFFER_SIZE);
        logger.info("[" + handlerKey.getHandle() + " thread " + threadId +
            "]: flushing to retry queue complete. Pending " + handlerKey.getEntityType() +
            ": " + datum.size());
      }
    }
  };

  abstract void flushSingleBatch(List<T> batch, @Nullable QueueingReason reason);

  public void drainBuffersToQueue(@Nullable QueueingReason reason) {
    if (isBuffering.compareAndSet(false, true)) {
      bufferFlushCounter.inc();
      try {
        int lastBatchSize = Integer.MIN_VALUE;
        // roughly limit number of items to flush to the the current buffer size (+1 blockSize max)
        // if too many points arrive at the proxy while it's draining,
        // they will be taken care of in the next run
        int toFlush = datum.size();
        while (toFlush > 0) {
          List<T> batch = createBatch();
          int batchSize = batch.size();
          if (batchSize > 0) {
            flushSingleBatch(batch, reason);
            // update the counters as if this was a failed call to the API
            this.attemptedCounter.inc(batchSize);
            toFlush -= batchSize;
            // stop draining buffers if the batch is smaller than the previous one
            if (batchSize < lastBatchSize) {
              break;
            }
            lastBatchSize = batchSize;
          } else {
            break;
          }
        }
      } finally {
        isBuffering.set(false);
        bufferCompletedFlushCounter.inc();
      }
    }
  }

  @Override
  public long getTaskRelativeScore() {
    return datum.size() + (isBuffering.get() ? properties.getMemoryBufferLimit() :
        (isSending ? properties.getItemsPerBatch() / 2 : 0));
  }
}
