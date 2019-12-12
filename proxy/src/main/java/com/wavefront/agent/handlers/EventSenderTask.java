package com.wavefront.agent.handlers;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.config.ProxyRuntimeSettings;
import com.wavefront.agent.data.EventDataSubmissionTask;
import com.wavefront.agent.data.TaskQueueingDirective;
import com.wavefront.agent.data.TaskResult;
import com.wavefront.agent.queueing.TaskQueue;
import com.wavefront.api.EventAPI;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.dto.Event;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import wavefront.report.ReportEvent;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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
  @SuppressWarnings("UnstableApiUsage")
  private final RateLimiter warningMessageRateLimiter = RateLimiter.create(0.1);

  private final Timer batchSendTime;

  private final EventAPI proxyAPI;
  private final UUID proxyId;
  private final ProxyRuntimeSettings runtimeSettings;
  private final RecyclableRateLimiter rateLimiter;
  private final Counter permitsGranted;
  private final Counter permitsDenied;
  private final Counter permitsRetried;
  private final TaskQueue<EventDataSubmissionTask> backlog;

  /**
   * Create new instance
   *
   * @param proxyAPI          handles interaction with Wavefront servers as well as queueing.
   * @param proxyId           id of the proxy.
   * @param handle            handle (usually port number), that serves as an identifier for the metrics pipeline.
   * @param threadId          thread number.
   * @param runtimeSettings   container for mutable proxy settings.
   * @param rateLimiter       rate limiter to control outbound point rate.
   * @param backlog           backing queue
   */
  EventSenderTask(EventAPI proxyAPI, UUID proxyId, String handle, int threadId,
                  ProxyRuntimeSettings runtimeSettings,
                  @Nullable RecyclableRateLimiter rateLimiter,
                  TaskQueue<EventDataSubmissionTask> backlog) {
    super(ReportableEntityType.EVENT, handle, threadId,
        runtimeSettings.getItemsPerBatchForEntityType(ReportableEntityType.EVENT),
        runtimeSettings.getMemoryBufferLimitForEntityType(ReportableEntityType.EVENT),
        rateLimiter);
    this.proxyAPI = proxyAPI;
    this.proxyId = proxyId;
    this.runtimeSettings = runtimeSettings;
    this.batchSendTime = Metrics.newTimer(new MetricName("api.events." + handle, "", "duration"),
        TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
    this.rateLimiter = rateLimiter;
    this.backlog = backlog;

    this.permitsGranted = Metrics.newCounter(new MetricName("limiter", "", "permits-granted"));
    this.permitsDenied = Metrics.newCounter(new MetricName("limiter", "", "permits-denied"));
    this.permitsRetried = Metrics.newCounter(new MetricName("limiter", "", "permits-retried"));

    this.scheduler.schedule(this, runtimeSettings.getPushFlushInterval(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void run() {
    long nextRunMillis = runtimeSettings.getPushFlushInterval();
    isSending = true;
    try {
      List<ReportEvent> current = createBatch();
      int batchSize = current.size();
      if (batchSize == 0) return;
      TimerContext timerContext = this.batchSendTime.time();
      if (rateLimiter == null || rateLimiter.tryAcquire(batchSize)) {
        if (rateLimiter != null) this.permitsGranted.inc(batchSize);
        try {
          EventDataSubmissionTask task = new EventDataSubmissionTask(proxyAPI, proxyId, handle,
              current.stream().map(Event::new).collect(Collectors.toList()), null);
          TaskResult result = task.execute(TaskQueueingDirective.DEFAULT, backlog);
          this.attemptedCounter.inc();
          if (result == TaskResult.PERSISTED) {
            if (rateLimiter != null) {
              this.rateLimiter.recyclePermits(batchSize);
            }
            this.queuedCounter.inc(batchSize);
          }
        } finally {
          timerContext.stop();
        }
      } else {
        permitsDenied.inc(batchSize);
        nextRunMillis = 250 + (int) (Math.random() * 250);
        //noinspection UnstableApiUsage
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
      EventDataSubmissionTask task = new EventDataSubmissionTask(proxyAPI, proxyId, handle,
          items.stream().map(Event::new).collect(Collectors.toList()), null);
      try {
        task.enqueue(backlog);
        this.attemptedCounter.inc(batchSize);
        this.queuedCounter.inc(batchSize);
      } catch (IOException e) {
        Metrics.newCounter(new TaggedMetricName("buffer", "failures", "port", handle)).inc();
        logger.severe("CRITICAL (Losing events!): WF-1: Error adding task to the queue: " +
            e.getMessage());
      }
      toFlush -= batchSize;

      // stop draining buffers if the batch is smaller than the previous one
      if (batchSize < lastBatchSize) {
        break;
      }
      lastBatchSize = batchSize;
    }
  }
}
