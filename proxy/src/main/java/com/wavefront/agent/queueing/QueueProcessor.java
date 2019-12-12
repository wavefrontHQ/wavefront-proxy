package com.wavefront.agent.queueing;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.data.TaskInjector;
import com.wavefront.agent.data.TaskQueueingDirective;

import javax.annotation.Nullable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.wavefront.agent.handlers.RecyclableRateLimiterFactoryImpl.UNLIMITED;

/**
 * A queue processor thread
 *
 * @param <T>
 *
 * @author vasily@wavefront.com
 */
public class QueueProcessor<T extends DataSubmissionTask<T>> implements Runnable {
  protected static final Logger logger = Logger.getLogger(QueueProcessor.class.getCanonicalName());

  protected final String handle;
  protected final TaskQueue<T> taskQueue;
  protected final ScheduledExecutorService executorService;
  protected final TaskInjector<T> taskInjector;
  protected final boolean splitPushWhenRateLimited;
  protected final int minSplitSize;
  protected final int flushInterval;
  protected final Supplier<Double> retryBackoffBaseSeconds;
  protected final RecyclableRateLimiter rateLimiter;
  protected volatile long lastProcessedTs;
  private int backoffExponent = 1;

  /**
   *
   *
   * @param handle                   pipeline handle
   * @param taskQueue                backing queue
   * @param executorService          executor service used to run tasks
   * @param taskInjector             injects members into task objects after deserialization
   * @param splitPushWhenRateLimited whether to split batches and retry immediately on pushback
   * @param minSplitSize             don't split batches if at or below this threshold
   * @param flushInterval            flush frequency
   * @param retryBackoffBaseSeconds  base for exponential back-off
   * @param rateLimiter              optional rate limiter
   */
  public QueueProcessor(final String handle,
                        final TaskQueue<T> taskQueue,
                        final ScheduledExecutorService executorService,
                        final TaskInjector<T> taskInjector,
                        final boolean splitPushWhenRateLimited,
                        final int minSplitSize,
                        final int flushInterval,
                        final Supplier<Double> retryBackoffBaseSeconds,
                        @Nullable final RecyclableRateLimiter rateLimiter) {
    this.handle = handle;
    this.taskQueue = taskQueue;
    this.executorService = executorService;
    this.taskInjector = taskInjector;
    this.splitPushWhenRateLimited = splitPushWhenRateLimited;
    this.minSplitSize = minSplitSize;
    this.flushInterval = flushInterval;
    this.retryBackoffBaseSeconds = retryBackoffBaseSeconds;
    this.rateLimiter = rateLimiter == null ? UNLIMITED : rateLimiter;
  }

  @Override
  public void run() {
    int successes = 0;
    int failures = 0;
    boolean rateLimiting = false;
    try {
      while (taskQueue.size() > 0 && taskQueue.size() > failures) {
        if (Thread.currentThread().isInterrupted()) return;
        T task = taskQueue.peek();
        int taskSize = task == null ? 0 : task.weight();
        int permitsNeeded = Math.max((int) rateLimiter.getRate(), taskSize);
        if (rateLimiter.immediatelyAvailable(permitsNeeded)) {
          // if there's less than 1 second worth of accumulated credits, don't process the backlog queue
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
            task.execute(TaskQueueingDirective.DEFAULT, taskQueue);
            successes++;
          }
        } catch (Exception ex) {
          rateLimiter.recyclePermits(taskSize);
          failures++;
          if (Throwables.getRootCause(ex) instanceof QueuedPushTooLargeException) {
            // this should split this task, remove it from the queue, and not try more tasks
            logger.warning("Wavefront server rejected push with HTTP 413: request too large, " +
                "will retry with smaller batch size.");
            for (T smallerTask : task.splitTask(minSplitSize)) {
              taskQueue.add(smallerTask);
            }
            break;
          } else if (Throwables.getRootCause(ex) instanceof RejectedExecutionException) {
              // this should either split and remove the original task or keep it at front
              // it also should not try any more tasks
              logger.warning("Wavefront server rejected push " +
                  "(global rate limit exceeded) - will attempt later.");
              if (splitPushWhenRateLimited) {
                for (T smallerTask : task.splitTask(minSplitSize)) {
                  taskQueue.add(smallerTask);
                }
              } else {
                removeTask = false;
              }
              break;
            } else {
              // TODO: more user-friendly messages for IO errors
              logger.log(Level.WARNING, "Cannot submit data to Wavefront servers. Will " +
                  "re-attempt later", Throwables.getRootCause(ex));
            }
          // this can potentially cause a duplicate task to be injected (but since submission is mostly
          // idempotent it's not really a big deal)
          taskQueue.add(task);
          if (failures > 10) {
            logger.warning("Too many submission errors, will re-attempt later");
            break;
          }
        } finally {
          if (removeTask) {
            taskQueue.remove();
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
        nextFlush = (flushInterval / 4) + (int) (Math.random() * (flushInterval / 4));
      } else {
        if (successes == 0 && failures != 0) {
          backoffExponent = Math.min(4, backoffExponent + 1); // caps at 2*base^4
        } else {
          backoffExponent = 1;
        }
        nextFlush = (long) ((Math.random() + 1.0) * 1000 * Math.pow(retryBackoffBaseSeconds.get(),
            backoffExponent));
        logger.fine("Next run scheduled in " + nextFlush + "ms");
      }
      executorService.schedule(this, nextFlush, TimeUnit.MILLISECONDS);
    }
  }
}
