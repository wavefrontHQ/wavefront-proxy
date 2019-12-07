package com.wavefront.agent.handlers;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.RecyclableRateLimiter;

import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.data.LineDelimitedDataSubmissionTask;
import com.wavefront.agent.data.TaskQueueingDirective;
import com.wavefront.agent.data.TaskResult;
import com.wavefront.agent.queueing.TaskSizeEstimator;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.api.ProxyV2API;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

/**
 * SenderTask for newline-delimited data.
 *
 * @author vasily@wavefront.com
 */
class LineDelimitedSenderTask extends AbstractSenderTask<String> {

  private static final Logger logger = Logger.getLogger(LineDelimitedSenderTask.class.getCanonicalName());

  private final String pushFormat;

  /**
   * Warn about exceeding the rate limit no more than once per 10 seconds (per thread)
   */
  private final RateLimiter warningMessageRateLimiter = RateLimiter.create(0.1);

  private final RecyclableRateLimiter pushRateLimiter;
  private final TaskSizeEstimator taskSizeEstimator;
  private final TaskQueue<LineDelimitedDataSubmissionTask> backlog;

  private final Counter batchesSuccessful;
  private final Counter batchesFailed;

  private final AtomicInteger pushFlushInterval;

  private ProxyV2API proxyAPI;
  private UUID proxyId;


  /**
   * Create new LineDelimitedSenderTask instance.
   *
   * @param entityType        entity type that dictates the data processing flow.
   * @param pushFormat        format parameter passed to the API endpoint.
   * @param proxyAPI          handles interaction with Wavefront servers as well as queueing.
   * @param proxyId           proxy ID.
   * @param handle            handle (usually port number), that serves as an identifier for the
   *                          metrics pipeline.
   * @param threadId          thread number.
   * @param pushRateLimiter   rate limiter to control outbound point rate.
   * @param pushFlushInterval interval between flushes.
   * @param itemsPerBatch     max points per flush.
   * @param memoryBufferLimit max points in task's memory buffer before queueing.
   * @param taskSizeEstimator
   * @param backlog
   */
  LineDelimitedSenderTask(ReportableEntityType entityType, String pushFormat,
                          ProxyV2API proxyAPI, UUID proxyId, String handle,
                          int threadId, final RecyclableRateLimiter pushRateLimiter,
                          final AtomicInteger pushFlushInterval,
                          @Nullable final AtomicInteger itemsPerBatch,
                          @Nullable final AtomicInteger memoryBufferLimit,
                          TaskSizeEstimator taskSizeEstimator,
                          TaskQueue<LineDelimitedDataSubmissionTask> backlog) {
    super(entityType, handle, threadId, itemsPerBatch, memoryBufferLimit, pushRateLimiter);
    this.pushFormat = pushFormat;
    this.proxyId = proxyId;
    this.pushFlushInterval = pushFlushInterval;
    this.proxyAPI = proxyAPI;
    this.pushRateLimiter = pushRateLimiter;
    this.taskSizeEstimator = taskSizeEstimator;
    this.backlog = backlog;

    this.batchesSuccessful = Metrics.newCounter(new MetricName("push." + handle, "", "batches"));
    this.batchesFailed = Metrics.newCounter(new MetricName("push." + handle, "", "batches-errors"));

    this.scheduler.schedule(this, pushFlushInterval.get(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void run() {
    long nextRunMillis = this.pushFlushInterval.get();
    isSending = true;
    try {
      List<String> current = createBatch();
      int currentBatchSize = current.size();
      if (currentBatchSize == 0) {
        return;
      }
      if (pushRateLimiter == null || pushRateLimiter.tryAcquire(currentBatchSize)) {
        LineDelimitedDataSubmissionTask task = new LineDelimitedDataSubmissionTask(proxyAPI,
            proxyId, pushFormat, entityType, handle, current, null);
        taskSizeEstimator.scheduleTaskForSizing(task);
        TaskResult result = task.execute(TaskQueueingDirective.DEFAULT, backlog);
        this.attemptedCounter.inc(currentBatchSize);
        switch (result) {
          case COMPLETE:
            break;
          case PERSISTED:
            if (pushRateLimiter != null) {
              this.pushRateLimiter.recyclePermits(currentBatchSize);
            }
            this.queuedCounter.inc(currentBatchSize);
            break;
          case NOT_PERSISTED:
            undoBatch(current);
          case FORCE_PERSISTED:
          default:
        }
      } else {
        // if proxy rate limit exceeded, try again in 1/4..1/2 of flush interval
        // to introduce some degree of fairness.
        nextRunMillis = nextRunMillis / 4 + (int) (Math.random() * nextRunMillis / 4);
        if (warningMessageRateLimiter.tryAcquire()) {
          logger.info("[" + handle + " thread " + threadId + "]: WF-4 Proxy rate limiter " +
              "active (pending " + entityType + ": " + datum.size() + "), will retry in " +
              nextRunMillis + "ms");
        }
        undoBatch(current);
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
      List<String> pushData = createBatch();
      int pushDataPointCount = pushData.size();
      if (pushDataPointCount > 0) {
        LineDelimitedDataSubmissionTask task = new LineDelimitedDataSubmissionTask(proxyAPI,
            proxyId, pushFormat, entityType, handle, pushData, null);
        try {
          task.enqueue(backlog);
          // update the counters as if this was a failed call to the API
          this.attemptedCounter.inc(pushDataPointCount);
          this.queuedCounter.inc(pushDataPointCount);
        } catch (IOException e) {
          Metrics.newCounter(new TaggedMetricName("buffer", "failures", "port", handle)).inc();
          logger.severe("CRITICAL (Losing points!): WF-1: Error adding task to the queue: " +
              e.getMessage());
        }
        toFlush -= pushDataPointCount;

        // stop draining buffers if the batch is smaller than the previous one
        if (pushDataPointCount < lastBatchSize) {
          break;
        }
        lastBatchSize = pushDataPointCount;
      } else {
        break;
      }
    }
  }
}
