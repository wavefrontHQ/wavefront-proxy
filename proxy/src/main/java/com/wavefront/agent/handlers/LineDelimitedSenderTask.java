package com.wavefront.agent.handlers;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.RecyclableRateLimiter;

import com.wavefront.agent.api.ForceQueueEnabledAgentAPI;
import com.wavefront.api.agent.Constants;
import com.wavefront.ingester.StringLineIngester;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

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

  private final Counter permitsGranted;
  private final Counter permitsDenied;
  private final Counter permitsRetried;
  private final Counter batchesSuccessful;
  private final Counter batchesFailed;
  private final Timer batchSendTime;

  private final AtomicInteger pushFlushInterval;

  private ForceQueueEnabledAgentAPI proxyAPI;
  private UUID proxyId;


  /**
   * Create new LineDelimitedSenderTask instance.
   *
   * @param entityType        entity type that dictates the data processing flow.
   * @param pushFormat        format parameter passed to the API endpoint.
   * @param proxyAPI          handles interaction with Wavefront servers as well as queueing.
   * @param proxyId           proxy ID.
   * @param handle            handle (usually port number), that serves as an identifier for the metrics pipeline.
   * @param threadId          thread number.
   * @param pushRateLimiter   rate limiter to control outbound point rate.
   * @param pushFlushInterval interval between flushes.
   * @param itemsPerBatch     max points per flush.
   * @param memoryBufferLimit max points in task's memory buffer before queueing.
   */
  LineDelimitedSenderTask(String entityType, String pushFormat, ForceQueueEnabledAgentAPI proxyAPI,
                          UUID proxyId, String handle, int threadId,
                          final RecyclableRateLimiter pushRateLimiter,
                          final AtomicInteger pushFlushInterval,
                          @Nullable final AtomicInteger itemsPerBatch,
                          @Nullable final AtomicInteger memoryBufferLimit) {
    super(entityType, handle, threadId, itemsPerBatch, memoryBufferLimit);
    this.pushFormat = pushFormat;
    this.proxyId = proxyId;
    this.pushFlushInterval = pushFlushInterval;
    this.proxyAPI = proxyAPI;
    this.pushRateLimiter = pushRateLimiter;


    this.permitsGranted = Metrics.newCounter(new MetricName("limiter", "", "permits-granted"));
    this.permitsDenied = Metrics.newCounter(new MetricName("limiter", "", "permits-denied"));
    this.permitsRetried = Metrics.newCounter(new MetricName("limiter", "", "permits-retried"));
    this.batchesSuccessful = Metrics.newCounter(new MetricName("push." + handle, "", "batches"));
    this.batchesFailed = Metrics.newCounter(new MetricName("push." + handle, "", "batches-errors"));
    this.batchSendTime = Metrics.newTimer(new MetricName("push." + handle, "", "duration"),
        TimeUnit.MILLISECONDS, TimeUnit.MINUTES);

    this.scheduler.schedule(this, pushFlushInterval.get(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void run() {
    long nextRunMillis = this.pushFlushInterval.get();
    isSending = true;
    try {
      List<String> current = createBatch();
      if (current.size() == 0) {
        return;
      }
      if (pushRateLimiter == null || pushRateLimiter.tryAcquire(current.size())) {
        if (pushRateLimiter != null) this.permitsGranted.inc(current.size());

        TimerContext timerContext = this.batchSendTime.time();
        Response response = null;
        try {
          response = proxyAPI.postPushData(
              proxyId,
              Constants.GRAPHITE_BLOCK_WORK_UNIT,
              System.currentTimeMillis(),
              pushFormat,
              StringLineIngester.joinPushData(current));
          int itemsInList = current.size();
          this.attemptedCounter.inc(itemsInList);
          if (response.getStatus() == Response.Status.NOT_ACCEPTABLE.getStatusCode()) {
            if (pushRateLimiter != null) {
              this.pushRateLimiter.recyclePermits(itemsInList);
              this.permitsRetried.inc(itemsInList);
            }
            this.queuedCounter.inc(itemsInList);
          }
        } finally {
          timerContext.stop();
          if (response != null) response.close();
        }
      } else {
        this.permitsDenied.inc(current.size());
        // if proxy rate limit exceeded, try again in 250..500ms (to introduce some degree of fairness)
        nextRunMillis = 250 + (int) (Math.random() * 250);
        if (warningMessageRateLimiter.tryAcquire()) {
          logger.warning("[" + handle + " thread " + threadId + "]: WF-4 Proxy rate limiter active " +
              "(pending " + entityType + ": " + datum.size() + "), will retry");
        }
        synchronized (mutex) { // return the batch to the beginning of the queue
          datum.addAll(0, current);
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
  public void drainBuffersToQueue() {
    int lastBatchSize = Integer.MIN_VALUE;
    // roughly limit number of points to flush to the the current buffer size (+1 blockSize max)
    // if too many points arrive at the proxy while it's draining, they will be taken care of in the next run
    int toFlush = datum.size();
    while (toFlush > 0) {
      List<String> pushData = createBatch();
      int pushDataPointCount = pushData.size();
      if (pushDataPointCount > 0) {
        proxyAPI.postPushData(proxyId, Constants.GRAPHITE_BLOCK_WORK_UNIT,
            System.currentTimeMillis(), pushFormat,
            StringLineIngester.joinPushData(pushData), true);

        // update the counters as if this was a failed call to the API
        this.attemptedCounter.inc(pushDataPointCount);
        this.queuedCounter.inc(pushDataPointCount);
        if (pushRateLimiter != null) {
          this.permitsDenied.inc(pushDataPointCount);
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
