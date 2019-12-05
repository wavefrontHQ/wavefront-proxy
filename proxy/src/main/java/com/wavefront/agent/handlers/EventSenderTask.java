package com.wavefront.agent.handlers;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.RecyclableRateLimiter;

import com.wavefront.agent.api.ForceQueueEnabledProxyAPI;
import com.wavefront.dto.Event;
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
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

import wavefront.report.ReportEvent;

/**
 * This class is responsible for accumulating events and sending them batch. This
 * class is similar to PostPushDataTimedTask.
 *
 * @author vasily@wavefront.com
 */
class EventSenderTask extends AbstractSenderTask<ReportEvent> {
  private static final Logger logger = Logger.getLogger(EventSenderTask.class.getCanonicalName());

  /**
   * Warn about exceeding the rate limit no more than once per 10 seconds (per thread)
   */
  private final RateLimiter warningMessageRateLimiter = RateLimiter.create(0.1);

  private final Timer batchSendTime;

  private final ForceQueueEnabledProxyAPI proxyAPI;
  private final UUID proxyId;
  private final AtomicInteger pushFlushInterval;
  private final RecyclableRateLimiter rateLimiter;
  private final Counter permitsGranted;
  private final Counter permitsDenied;
  private final Counter permitsRetried;

  /**
   * Create new instance
   *
   * @param proxyAPI          handles interaction with Wavefront servers as well as queueing.
   * @param proxyId           id of the proxy.
   * @param handle            handle (usually port number), that serves as an identifier for the metrics pipeline.
   * @param threadId          thread number.
   * @param rateLimiter       rate limiter to control outbound point rate.
   * @param pushFlushInterval interval between flushes.
   * @param itemsPerBatch     max points per flush.
   * @param memoryBufferLimit max points in task's memory buffer before queueing.
   *
   */
  EventSenderTask(ForceQueueEnabledProxyAPI proxyAPI, UUID proxyId, String handle, int threadId,
                  AtomicInteger pushFlushInterval,
                  @Nullable RecyclableRateLimiter rateLimiter,
                  @Nullable AtomicInteger itemsPerBatch,
                  @Nullable AtomicInteger memoryBufferLimit) {
    super("events", handle, threadId, itemsPerBatch, memoryBufferLimit);
    this.proxyAPI = proxyAPI;
    this.proxyId = proxyId;
    this.batchSendTime = Metrics.newTimer(new MetricName("api.events." + handle, "", "duration"),
        TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
    this.pushFlushInterval = pushFlushInterval;
    this.rateLimiter = rateLimiter;

    this.permitsGranted = Metrics.newCounter(new MetricName("limiter", "", "permits-granted"));
    this.permitsDenied = Metrics.newCounter(new MetricName("limiter", "", "permits-denied"));
    this.permitsRetried = Metrics.newCounter(new MetricName("limiter", "", "permits-retried"));

    this.scheduler.schedule(this, this.pushFlushInterval.get(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void run() {
    long nextRunMillis = this.pushFlushInterval.get();
    isSending = true;
    try {
      List<ReportEvent> current = createBatch();
      int batchSize = current.size();
      if (batchSize == 0) return;
      Response response = null;
      TimerContext timerContext = this.batchSendTime.time();
      if (rateLimiter == null || rateLimiter.tryAcquire()) {
        if (rateLimiter != null) this.permitsGranted.inc(current.size());
        try {
          response = proxyAPI.proxyEvents(proxyId, current.stream().map(Event::new).
              collect(Collectors.toList()), false);
          this.attemptedCounter.inc(batchSize);
          if (response != null &&
              response.getStatus() == Response.Status.NOT_ACCEPTABLE.getStatusCode()) {
            if (rateLimiter != null) {
              this.rateLimiter.recyclePermits(batchSize);
              this.permitsRetried.inc(batchSize);
            }
            this.queuedCounter.inc(batchSize);
          }
        } finally {
          timerContext.stop();
          if (response != null) response.close();
        }
      } else {
        permitsDenied.inc(current.size());
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
  public void drainBuffersToQueueInternal() {
    int lastBatchSize = Integer.MIN_VALUE;
    // roughly limit number of points to flush to the the current buffer size (+1 blockSize max)
    // if too many points arrive at the proxy while it's draining, they will be taken care of in the next run
    int toFlush = datum.size();
    while (toFlush > 0) {
      List<ReportEvent> items = createBatch();
      int batchSize = items.size();
      if (batchSize == 0) return;
      proxyAPI.proxyEvents(proxyId, items.stream().map(Event::new).collect(Collectors.toList()),
          true);
      this.attemptedCounter.inc(items.size());
      this.queuedCounter.inc(items.size());
      toFlush -= batchSize;

      // stop draining buffers if the batch is smaller than the previous one
      if (batchSize < lastBatchSize) {
        break;
      }
      lastBatchSize = batchSize;
    }
  }
}
