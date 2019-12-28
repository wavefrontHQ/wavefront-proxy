package com.wavefront.agent.handlers;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.data.EntityWrapper.EntityProperties;
import com.wavefront.agent.data.SourceTagSubmissionTask;
import com.wavefront.agent.data.QueueingReason;
import com.wavefront.agent.data.TaskResult;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.api.SourceTagAPI;
import com.wavefront.data.ReportableEntityType;
import wavefront.report.ReportSourceTag;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is responsible for accumulating the source tag changes and post it in a batch. This
 * class is similar to PostPushDataTimedTask.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com)
 * @author vasily@wavefront.com
 */
class ReportSourceTagSenderTask extends AbstractSenderTask<ReportSourceTag> {
  private static final Logger logger =
      Logger.getLogger(ReportSourceTagSenderTask.class.getCanonicalName());

  /**
   * Warn about exceeding the rate limit no more than once per 10 seconds (per thread)
   */
  @SuppressWarnings("UnstableApiUsage")
  private final RateLimiter warningMessageRateLimiter = RateLimiter.create(0.1);

  private final SourceTagAPI proxyAPI;
  private final TaskQueue<SourceTagSubmissionTask> backlog;

  /**
   * Create new instance
   *
   * @param proxyAPI    handles interaction with Wavefront servers as well as queueing.
   * @param handle      handle (usually port number), that serves as an identifier
   *                    for the metrics pipeline.
   * @param threadId    thread number.
   * @param properties  container for mutable proxy settings.
   * @param rateLimiter rate limiter to control outbound point rate.
   * @param backlog     backing queue
   */
  ReportSourceTagSenderTask(SourceTagAPI proxyAPI, String handle, int threadId,
                            EntityProperties properties,
                            @Nullable RecyclableRateLimiter rateLimiter,
                            TaskQueue<SourceTagSubmissionTask> backlog) {
    super(ReportableEntityType.SOURCE_TAG, handle, threadId, properties, rateLimiter);
    this.proxyAPI = proxyAPI;
    this.backlog = backlog;
    this.scheduler.schedule(this, properties.getPushFlushInterval(), TimeUnit.MILLISECONDS);
  }

  @Override
  TaskResult processSingleBatch(List<ReportSourceTag> batch) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void run() {
    long nextRunMillis = properties.getPushFlushInterval();
    isSending = true;
    try {
      List<ReportSourceTag> current = createBatch();
      if (current.size() == 0) return;
      Iterator<ReportSourceTag> iterator = current.iterator();
      while (iterator.hasNext()) {
        if (rateLimiter == null || rateLimiter.tryAcquire()) {
          ReportSourceTag tag = iterator.next();
          SourceTagSubmissionTask task = new SourceTagSubmissionTask(proxyAPI, properties,
              backlog, handle, tag, null);
          TaskResult result = task.execute();
          this.attemptedCounter.inc();
          switch (result) {
            case DELIVERED:
              break;
            case PERSISTED:
            case PERSISTED_RETRY:
              if (rateLimiter != null) rateLimiter.recyclePermits(1);
              break;
            case RETRY_LATER:
              final List<ReportSourceTag> remainingItems = new ArrayList<>();
              remainingItems.add(tag);
              iterator.forEachRemaining(remainingItems::add);
              undoBatch(remainingItems);
              if (rateLimiter != null) rateLimiter.recyclePermits(1);
              return;
            default:
          }
        } else {
          final List<ReportSourceTag> remainingItems = new ArrayList<>();
          iterator.forEachRemaining(remainingItems::add);
          undoBatch(remainingItems);
          // if proxy rate limit exceeded, try again in 1/4..1/2 of flush interval
          // to introduce some degree of fairness.
          nextRunMillis = (int) (1 + Math.random()) * nextRunMillis / 4;
          //noinspection UnstableApiUsage
          if (warningMessageRateLimiter.tryAcquire()) {
            logger.info("[" + handle + " thread " + threadId + "]: WF-4 Proxy rate limiter " +
                "active (pending " + entityType + ": " + datum.size() + "), will retry in " +
                nextRunMillis + "ms");
          }
          return;
        }
      }
    } catch (Throwable t) {
      logger.log(Level.SEVERE, "Unexpected error in flush loop", t);
    } finally {
      isSending = false;
      scheduler.schedule(this, nextRunMillis, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  void flushSingleBatch(List<ReportSourceTag> batch, QueueingReason reason) {
    for (ReportSourceTag tag : batch) {
      SourceTagSubmissionTask task = new SourceTagSubmissionTask(proxyAPI, properties, backlog,
          handle, tag, null);
      task.enqueue(reason);
    }
  }
}
