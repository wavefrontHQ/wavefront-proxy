package com.wavefront.agent.handlers;

import com.google.common.util.concurrent.RateLimiter;

import com.wavefront.common.NamedThreadFactory;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

/**
 * Base class for all {@link SenderTask} implementations.
 *
 * @author vasily@wavefront.com
 *
 * @param <T> the type of input objects handled.
 */
abstract class AbstractSenderTask<T> implements SenderTask<T>, Runnable {
  private static final Logger logger = Logger.getLogger(AbstractSenderTask.class.getCanonicalName());

  List<T> datum = new ArrayList<>();
  final Object mutex = new Object();
  final ScheduledExecutorService scheduler;
  private final ExecutorService flushExecutor;

  final String entityType;
  protected final String handle;
  final int threadId;

  final AtomicInteger itemsPerBatch;
  final AtomicInteger memoryBufferLimit;

  final Counter receivedCounter;
  final Counter attemptedCounter;
  final Counter queuedCounter;
  final Counter blockedCounter;
  final Counter bufferFlushCounter;
  final Counter bufferCompletedFlushCounter;

  AtomicBoolean isBuffering = new AtomicBoolean(false);
  boolean isSending = false;

  /**
   * Attempt to schedule drainBuffersToQueueTask no more than once every 100ms to reduce
   * scheduler overhead under memory pressure.
   */
  private final RateLimiter drainBuffersRateLimiter = RateLimiter.create(10);


  /**
   * Base constructor.
   *
   * @param entityType        entity type that dictates the data processing flow.
   * @param handle            handle (usually port number), that serves as an identifier for the metrics pipeline.
   * @param threadId          thread number
   * @param itemsPerBatch     max points per flush.
   * @param memoryBufferLimit max points in task's memory buffer before queueing.
   */
  AbstractSenderTask(String entityType, String handle, int threadId,
                     @Nullable final AtomicInteger itemsPerBatch,
                     @Nullable final AtomicInteger memoryBufferLimit) {
    this.entityType = entityType;
    this.handle = handle;
    this.threadId = threadId;
    this.itemsPerBatch = itemsPerBatch == null ? new AtomicInteger(40000) : itemsPerBatch;
    this.memoryBufferLimit = memoryBufferLimit == null ? new AtomicInteger(32 * 40000) : memoryBufferLimit;
    this.scheduler = Executors.newScheduledThreadPool(1,
        new NamedThreadFactory("submitter-" + entityType + "-" + handle + "-" + String.valueOf(threadId)));
    this.flushExecutor = new ThreadPoolExecutor(1, 1, 60L, TimeUnit.MINUTES, new SynchronousQueue<>(),
        new NamedThreadFactory("flush-" + entityType + "-" + handle + "-" + String.valueOf(threadId)));

    this.attemptedCounter = Metrics.newCounter(new MetricName(entityType + "." + handle, "", "sent"));
    this.queuedCounter = Metrics.newCounter(new MetricName(entityType + "." + handle, "", "queued"));
    this.blockedCounter = Metrics.newCounter(new MetricName(entityType + "." + handle, "", "blocked"));
    this.receivedCounter = Metrics.newCounter(new MetricName(entityType + "." + handle, "", "received"));
    this.bufferFlushCounter = Metrics.newCounter(new TaggedMetricName("buffer", "flush-count", "port", handle));
    this.bufferCompletedFlushCounter = Metrics.newCounter(new TaggedMetricName("buffer", "completed-flush-count",
        "port", handle));
  }

  /**
   * Shut down the scheduler for this task (prevent future scheduled runs)
   */
  @Override
  public void shutdown() {
    try {
      scheduler.shutdownNow();
      scheduler.awaitTermination(1000L, TimeUnit.MILLISECONDS);
    } catch (Throwable t) {
      logger.log(Level.SEVERE, "Error during shutdown", t);
    }
  }

  @Override
  public void add(T metricString) {
    synchronized (mutex) {
      this.datum.add(metricString);
    }
    this.enforceBufferLimits();
  }


  void enforceBufferLimits() {
    if (datum.size() >= memoryBufferLimit.get() && !isBuffering.get() && drainBuffersRateLimiter.tryAcquire()) {
      try {
        flushExecutor.submit(drainBuffersToQueueTask);
      } catch (RejectedExecutionException e) {
        // ignore - another task is already being executed
      }
    }
  }

  List<T> createBatch() {
    List<T> current;
    int blockSize;
    synchronized (mutex) {
      blockSize = Math.min(datum.size(), itemsPerBatch.get());
      current = datum.subList(0, blockSize);
      datum = new ArrayList<>(datum.subList(blockSize, datum.size()));
    }
    logger.fine("[" + handle + "] (DETAILED): sending " + current.size() + " valid " + entityType +
        "; in memory: " + this.datum.size() +
        "; total attempted: " + this.attemptedCounter.count() +
        "; total blocked: " + this.blockedCounter.count() +
        "; total queued: " + this.queuedCounter.count());
    return current;
  }

  private Runnable drainBuffersToQueueTask = new Runnable() {
    @Override
    public void run() {
      if (datum.size() > memoryBufferLimit.get()) {
        // there are going to be too many points to be able to flush w/o the agent blowing up
        // drain the leftovers straight to the retry queue (i.e. to disk)
        // don't let anyone add any more to points while we're draining it.
        logger.warning("[" + handle + " thread " + threadId + "]: WF-3 Too many pending " + entityType +
            " (" + datum.size() + "), block size: " + itemsPerBatch.get() + ". flushing to retry queue");
        drainBuffersToQueue();
        logger.info("[" + handle + " thread " + threadId + "]: flushing to retry queue complete. " +
            "Pending " + entityType + ": " + datum.size());
      }
    }
  };

  abstract void drainBuffersToQueueInternal();

  public void drainBuffersToQueue() {
    if (isBuffering.compareAndSet(false, true)) {
      bufferFlushCounter.inc();
      try {
        drainBuffersToQueueInternal();
      } finally {
        isBuffering.set(false);
        bufferCompletedFlushCounter.inc();
      }
    }
  }

  @Override
  public long getTaskRelativeScore() {
    return datum.size() + (isBuffering.get() ? memoryBufferLimit.get() : (isSending ? itemsPerBatch.get() / 2 : 0));
  }


}
