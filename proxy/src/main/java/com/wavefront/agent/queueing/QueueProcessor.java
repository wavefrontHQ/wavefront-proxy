package com.wavefront.agent.queueing;

import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.GlobalProperties;
import com.wavefront.common.Managed;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.data.TaskInjector;
import com.wavefront.agent.data.TaskResult;
import com.wavefront.agent.handlers.HandlerKey;

import javax.annotation.Nonnull;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A thread responsible for processing the backlog from a single task queue.
 *
 * @param <T> type of queued tasks
 *
 * @author vasily@wavefront.com
 */
public class QueueProcessor<T extends DataSubmissionTask<T>> implements Runnable, Managed {
  protected static final Logger logger = Logger.getLogger(QueueProcessor.class.getCanonicalName());

  protected final HandlerKey handlerKey;
  protected final TaskQueue<T> taskQueue;
  protected final ScheduledExecutorService scheduler;
  private final GlobalProperties globalProps;
  protected final TaskInjector<T> taskInjector;
  protected final EntityProperties runtimeProperties;
  protected final RecyclableRateLimiter rateLimiter;
  private volatile long headTaskTimestamp = Long.MAX_VALUE;
  private volatile double schedulerTimingFactor = 1.0d;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private int backoffExponent = 1;
  private Supplier<T> storedTask;

  /**
   * @param handlerKey         pipeline handler key
   * @param taskQueue          backing queue
   * @param taskInjector       injects members into task objects after deserialization
   * @param entityProps        container for mutable proxy settings.
   * @param globalProps        container for mutable global proxy settings.
   */
  public QueueProcessor(final HandlerKey handlerKey,
                        @Nonnull final TaskQueue<T> taskQueue,
                        final TaskInjector<T> taskInjector,
                        final ScheduledExecutorService scheduler,
                        final EntityProperties entityProps,
                        final GlobalProperties globalProps) {
    this.handlerKey = handlerKey;
    this.taskQueue = taskQueue;
    this.taskInjector = taskInjector;
    this.runtimeProperties = entityProps;
    this.rateLimiter = entityProps.getRateLimiter();
    this.scheduler = scheduler;
    this.globalProps = globalProps;
  }

  @Override
  public void run() {
    if (!isRunning.get()) return;
    int successes = 0;
    int failures = 0;
    boolean rateLimiting = false;
    try {
      while (taskQueue.size() > 0 && taskQueue.size() > failures) {
        if (!isRunning.get() || Thread.currentThread().isInterrupted()) return;
        if (storedTask == null) {
          storedTask = Suppliers.memoizeWithExpiration(taskQueue::peek, 500, TimeUnit.MILLISECONDS);
        }
        T task = storedTask.get();
        int taskSize = task == null ? 0 : task.weight();
        this.headTaskTimestamp = task == null ? Long.MAX_VALUE : task.getEnqueuedMillis();
        int permitsNeeded = Math.min((int) rateLimiter.getRate(), taskSize);
        if (!rateLimiter.immediatelyAvailable(permitsNeeded)) {
          // if there's less than 1 second worth of accumulated credits,
          // don't process the backlog queue
          rateLimiting = true;
          break;
        }
        if (taskSize > 0) {
          rateLimiter.acquire(taskSize);
        }
        boolean removeTask = true;
        try {
          if (task != null) {
            taskInjector.inject(task);
            TaskResult result = task.execute();
            switch (result) {
              case DELIVERED:
                successes++;
                break;
              case PERSISTED:
                rateLimiter.recyclePermits(taskSize);
                failures++;
                return;
              case PERSISTED_RETRY:
                rateLimiter.recyclePermits(taskSize);
                failures++;
                break;
              case RETRY_LATER:
                removeTask = false;
                rateLimiter.recyclePermits(taskSize);
                failures++;
            }
          }
          if (failures >= 10) {
            break;
          }
        } finally {
          if (removeTask) {
            taskQueue.remove();
            if (taskQueue.size() == 0) schedulerTimingFactor = 1.0d;
            storedTask = null;
          }
        }
      }
      if (taskQueue.size() == 0) headTaskTimestamp = Long.MAX_VALUE;
    } catch (Throwable ex) {
      logger.log(Level.WARNING, "Unexpected exception", ex);
    } finally {
      long nextFlush;
      if (rateLimiting) {
        logger.fine("[" + handlerKey.getHandle() + "] Rate limiter active, will re-attempt later " +
            "to prioritize eal-time traffic.");
        // if proxy rate limit exceeded, try again in 1/4 to 1/2 flush interval
        // (to introduce some degree of fairness)
        nextFlush = (int) ((1 + Math.random()) * runtimeProperties.getPushFlushInterval() / 4 *
            schedulerTimingFactor);
      } else {
        if (successes == 0 && failures > 0) {
          backoffExponent = Math.min(4, backoffExponent + 1); // caps at 2*base^4
        } else {
          backoffExponent = 1;
        }
        nextFlush = (long) ((Math.random() + 1.0) * runtimeProperties.getPushFlushInterval() *
            Math.pow(globalProps.getRetryBackoffBaseSeconds(),
                backoffExponent) * schedulerTimingFactor);
        logger.fine("[" + handlerKey.getHandle() + "] Next run scheduled in " + nextFlush + "ms");
      }
      if (isRunning.get()) {
        scheduler.schedule(this, nextFlush, TimeUnit.MILLISECONDS);
      }
    }
  }

  @Override
  public void start() {
    if (isRunning.compareAndSet(false, true)) {
      scheduler.submit(this);
    }
  }

  @Override
  public void stop() {
    isRunning.set(false);
  }

  /**
   * Returns the timestamp of the task at the head of the queue.
   * @return timestamp
   */
  long getHeadTaskTimestamp() {
    return this.headTaskTimestamp;
  }

  /**
   * Returns the backing queue.
   * @return task queue
   */
  TaskQueue<T> getTaskQueue() {
    return this.taskQueue;
  }

  /**
   * Adjusts the timing multiplier for this processor. If the timingFactor value is lower than 1,
   * delays between cycles get shorter which results in higher priority for the queue;
   * if it's higher than 1, delays get longer, which, naturally, lowers the priority.
   * @param timingFactor timing multiplier
   */
  void setTimingFactor(double timingFactor) {
    this.schedulerTimingFactor = timingFactor;
  }
}
