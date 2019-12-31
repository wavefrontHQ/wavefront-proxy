package com.wavefront.agent.queueing;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.Managed;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.data.EntityWrapper.EntityProperties;
import com.wavefront.agent.data.TaskInjector;
import com.wavefront.agent.data.TaskResult;
import com.wavefront.agent.handlers.HandlerKey;

import javax.annotation.Nullable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.wavefront.agent.handlers.RecyclableRateLimiterFactoryImpl.UNLIMITED;

/**
 * A queue processor thread.
 *
 * @param <T>
 *
 * @author vasily@wavefront.com
 */
public class QueueProcessor<T extends DataSubmissionTask<T>> implements Runnable, Managed {
  protected static final Logger logger = Logger.getLogger(QueueProcessor.class.getCanonicalName());

  protected final HandlerKey handlerKey;
  protected final TaskQueue<T> taskQueue;
  protected final ScheduledExecutorService scheduler;
  protected final TaskInjector<T> taskInjector;
  protected final EntityProperties runtimeProperties;
  protected final RecyclableRateLimiter rateLimiter;
  private volatile long lastSeenTimestamp = Long.MIN_VALUE;
  private volatile double schedulerPriorityFactor = 1.0d;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);
  private int backoffExponent = 1;

  /**
   * @param handlerKey               pipeline handler key
   * @param taskQueue                backing queue
   * @param taskInjector             injects members into task objects after deserialization
   * @param runtimeProperties        container for mutable proxy settings.
   * @param rateLimiter              optional rate limiter
   */
  public QueueProcessor(final HandlerKey handlerKey,
                        final TaskQueue<T> taskQueue,
                        final TaskInjector<T> taskInjector,
                        final ScheduledExecutorService scheduler,
                        final EntityProperties runtimeProperties,
                        @Nullable final RecyclableRateLimiter rateLimiter) {
    this.handlerKey = handlerKey;
    this.taskQueue = taskQueue;
    this.taskInjector = taskInjector;
    this.runtimeProperties = runtimeProperties;
    this.rateLimiter = rateLimiter == null ? UNLIMITED : rateLimiter;
    this.scheduler = scheduler;
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
        T task = taskQueue.peek();
        int taskSize = task == null ? 0 : task.weight();
        this.lastSeenTimestamp = task == null ? Long.MIN_VALUE : task.getCreatedMillis();
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
            if (taskQueue.size() == 0) schedulerPriorityFactor = 1.0d;
          }
        }
      }
    } catch (Throwable ex) {
      logger.log(Level.WARNING, "Unexpected exception", ex);
    } finally {
      long nextFlush;
      if (rateLimiting) {
        logger.fine("Rate limiter active, will re-attempt later to prioritize real-time traffic.");
        // if proxy rate limit exceeded, try again in 1/4 to 1/2 flush interval
        // (to introduce some degree of fairness)
        nextFlush = (int) ((1 + Math.random()) * runtimeProperties.getPushFlushInterval() / 4 *
            schedulerPriorityFactor);
      } else {
        if (successes == 0 && failures > 0) {
          backoffExponent = Math.min(4, backoffExponent + 1); // caps at 2*base^4
        } else {
          backoffExponent = 1;
        }
        nextFlush = (long) ((Math.random() + 1.0) * runtimeProperties.getPushFlushInterval() *
            Math.pow(runtimeProperties.getRetryBackoffBaseSeconds(), backoffExponent) *
            schedulerPriorityFactor);
        logger.fine("Next run scheduled in " + nextFlush + "ms");
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

  public long getLastSeenTimestamp() {
    return this.lastSeenTimestamp;
  }

  public void setTimingFactor(double priorityFactor) {
    this.schedulerPriorityFactor = priorityFactor;
  }
}
