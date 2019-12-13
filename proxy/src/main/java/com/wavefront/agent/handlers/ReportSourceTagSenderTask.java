package com.wavefront.agent.handlers;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.config.ProxyRuntimeProperties;
import com.wavefront.agent.data.SourceTagSubmissionTask;
import com.wavefront.agent.data.TaskQueueingDirective;
import com.wavefront.agent.data.TaskResult;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.api.SourceTagAPI;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import wavefront.report.ReportSourceTag;

import javax.annotation.Nullable;
import java.io.IOException;
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
  private static final Logger logger = Logger.getLogger(ReportSourceTagSenderTask.class.getCanonicalName());

  /**
   * Warn about exceeding the rate limit no more than once per 10 seconds (per thread)
   */
  @SuppressWarnings("UnstableApiUsage")
  private final RateLimiter warningMessageRateLimiter = RateLimiter.create(0.1);

  private final Timer batchSendTime;

  private final SourceTagAPI proxyAPI;
  private final ProxyRuntimeProperties runtimeProperties;
  private final RecyclableRateLimiter rateLimiter;
  private final TaskQueue<SourceTagSubmissionTask> backlog;

  /**
   * Create new instance
   *
   * @param proxyAPI          handles interaction with Wavefront servers as well as queueing.
   * @param handle            handle (usually port number), that serves as an identifier
   *                          for the metrics pipeline.
   * @param threadId          thread number.
   * @param runtimeProperties container for mutable proxy settings.
   * @param rateLimiter       rate limiter to control outbound point rate.
   * @param backlog           backing queue
   *
   */
  ReportSourceTagSenderTask(SourceTagAPI proxyAPI, String handle, int threadId,
                            ProxyRuntimeProperties runtimeProperties,
                            @Nullable RecyclableRateLimiter rateLimiter,
                            TaskQueue<SourceTagSubmissionTask> backlog) {
    super(ReportableEntityType.SOURCE_TAG, handle, threadId,
        runtimeProperties.getItemsPerBatchForEntityType(ReportableEntityType.SOURCE_TAG),
        runtimeProperties.getMemoryBufferLimitForEntityType(ReportableEntityType.SOURCE_TAG),
        rateLimiter);
    this.proxyAPI = proxyAPI;
    this.batchSendTime = Metrics.newTimer(new MetricName("api.sourceTags." + handle, "",
            "duration"), TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
    this.runtimeProperties = runtimeProperties;
    this.rateLimiter = rateLimiter;
    this.backlog = backlog;
    this.scheduler.schedule(this, runtimeProperties.getPushFlushInterval(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void run() {
    long nextRunMillis = runtimeProperties.getPushFlushInterval();
    isSending = true;
    try {
      List<ReportSourceTag> current = createBatch();
      if (current.size() == 0) return;
      boolean forceToQueue = false;
      Iterator<ReportSourceTag> iterator = current.iterator();
      while (iterator.hasNext()) {
        TimerContext timerContext = this.batchSendTime.time();
        if (rateLimiter == null || rateLimiter.tryAcquire()) {
          try {
            ReportSourceTag tag = iterator.next();
            SourceTagSubmissionTask task = new SourceTagSubmissionTask(proxyAPI, handle, tag, null);
            if (forceToQueue) {
              try {
                task.enqueue(backlog);
                // update the counters as if this was a failed call to the API
                this.attemptedCounter.inc();
                this.queuedCounter.inc();
              } catch (IOException e) {
                Metrics.newCounter(new TaggedMetricName("buffer", "failures", "port", handle)).inc();
                logger.severe("CRITICAL (Losing tags!): WF-1: Error adding task to the queue: " +
                    e.getMessage());
              }
            } else {
              TaskResult result = task.execute(TaskQueueingDirective.DEFAULT, backlog);
              this.attemptedCounter.inc();
              if (result == TaskResult.PERSISTED) {
                if (rateLimiter != null) {
                  this.rateLimiter.recyclePermits(1);
                }
                this.queuedCounter.inc();
                forceToQueue = true;
              }
            }
          } finally {
            timerContext.stop();
          }
        } else {
          final List<ReportSourceTag> remainingItems = new ArrayList<>();
          iterator.forEachRemaining(remainingItems::add);
          // if proxy rate limit exceeded, try again in 1/4..1/2 of flush interval
          // to introduce some degree of fairness.
          nextRunMillis = nextRunMillis / 4 + (int) (Math.random() * nextRunMillis / 4);
          //noinspection UnstableApiUsage
          if (warningMessageRateLimiter.tryAcquire()) {
            logger.info("[" + handle + " thread " + threadId + "]: WF-4 Proxy rate limiter " +
                "active (pending " + entityType + ": " + datum.size() + "), will retry in " +
                nextRunMillis + "ms");
          }
          undoBatch(remainingItems);
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
  void drainBuffersToQueueInternal() {
    int lastBatchSize = Integer.MIN_VALUE;
    // roughly limit number of points to flush to the the current buffer size (+1 blockSize max)
    // if too many points arrive at the proxy while it's draining,
    // they will be taken care of in the next run
    int toFlush = datum.size();
    while (toFlush > 0) {
      List<ReportSourceTag> pushData = createBatch();
      int numTags = pushData.size();
      if (numTags > 0) {
        for (ReportSourceTag tag : pushData) {
          SourceTagSubmissionTask task = new SourceTagSubmissionTask(proxyAPI, handle, tag, null);
          try {
            task.enqueue(backlog);
            // update the counters as if this was a failed call to the API
            this.attemptedCounter.inc(numTags);
            this.queuedCounter.inc(numTags);
          } catch (IOException e) {
            Metrics.newCounter(new TaggedMetricName("buffer", "failures", "port", handle)).inc();
            logger.severe("CRITICAL (Losing tags!): WF-1: Error adding task to the queue: " +
                e.getMessage());
          }
          toFlush--;
        }
        // stop draining buffers if the batch is smaller than the previous one
        if (numTags < lastBatchSize) {
          break;
        }
        lastBatchSize = numTags;
      } else {
        break;
      }
    }
  }
}
