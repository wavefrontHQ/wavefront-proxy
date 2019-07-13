package com.wavefront.agent.handlers;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.RecyclableRateLimiter;

import com.wavefront.agent.api.ForceQueueEnabledProxyAPI;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

import wavefront.report.ReportSourceTag;

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
  private final RateLimiter warningMessageRateLimiter = RateLimiter.create(0.1);

  private final Timer batchSendTime;

  private final ForceQueueEnabledProxyAPI proxyAPI;
  private final AtomicInteger pushFlushInterval;
  private final RecyclableRateLimiter rateLimiter;
  private final Counter permitsGranted;
  private final Counter permitsDenied;
  private final Counter permitsRetried;

  /**
   * Create new instance
   *
   * @param proxyAPI          handles interaction with Wavefront servers as well as queueing.
   * @param handle            handle (usually port number), that serves as an identifier for the metrics pipeline.
   * @param threadId          thread number.
   * @param rateLimiter       rate limiter to control outbound point rate.
   * @param pushFlushInterval interval between flushes.
   * @param itemsPerBatch     max points per flush.
   * @param memoryBufferLimit max points in task's memory buffer before queueing.
   *
   */
  ReportSourceTagSenderTask(ForceQueueEnabledProxyAPI proxyAPI, String handle, int threadId,
                            AtomicInteger pushFlushInterval,
                            @Nullable RecyclableRateLimiter rateLimiter,
                            @Nullable AtomicInteger itemsPerBatch,
                            @Nullable AtomicInteger memoryBufferLimit) {
    super("sourceTags", handle, threadId, itemsPerBatch, memoryBufferLimit);
    this.proxyAPI = proxyAPI;
    this.batchSendTime = Metrics.newTimer(new MetricName("api.sourceTags." + handle, "", "duration"),
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
      List<ReportSourceTag> current = createBatch();
      if (current.size() == 0) return;
      Response response = null;
      boolean forceToQueue = false;
      Iterator<ReportSourceTag> iterator = current.iterator();
      while (iterator.hasNext()) {
        TimerContext timerContext = this.batchSendTime.time();
        if (rateLimiter == null || rateLimiter.tryAcquire()) {
          if (rateLimiter != null) this.permitsGranted.inc();
          ReportSourceTag sourceTag = iterator.next();

          try {
            response = executeSourceTagAction(proxyAPI, sourceTag, forceToQueue);
            this.attemptedCounter.inc();
            if (response != null && response.getStatus() == Response.Status.NOT_ACCEPTABLE.getStatusCode()) {
              if (rateLimiter != null) {
                this.rateLimiter.recyclePermits(1);
                this.permitsRetried.inc(1);
              }
              this.queuedCounter.inc();
              forceToQueue = true;
            }
          } finally {
            timerContext.stop();
            if (response != null) response.close();
          }
        } else {
          final List<ReportSourceTag> remainingItems = new ArrayList<>();
          iterator.forEachRemaining(remainingItems::add);
          permitsDenied.inc(remainingItems.size());
          nextRunMillis = 250 + (int) (Math.random() * 250);
          if (warningMessageRateLimiter.tryAcquire()) {
            logger.warning("[" + handle + " thread " + threadId + "]: WF-4 Proxy rate limiter active " +
                "(pending " + entityType + ": " + datum.size() + "), will retry");
          }
          synchronized (mutex) { // return the batch to the beginning of the queue
            datum.addAll(0, remainingItems);
          }
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
      List<ReportSourceTag> items = createBatch();
      int batchSize = items.size();
      if (batchSize == 0) return;
      for (ReportSourceTag sourceTag : items) {
        executeSourceTagAction(proxyAPI, sourceTag, true);
        this.attemptedCounter.inc();
        this.queuedCounter.inc();
      }
      toFlush -= batchSize;

      // stop draining buffers if the batch is smaller than the previous one
      if (batchSize < lastBatchSize) {
        break;
      }
      lastBatchSize = batchSize;
    }
  }

  @Nullable
  protected static Response executeSourceTagAction(ForceQueueEnabledProxyAPI wavefrontAPI, ReportSourceTag sourceTag,
                                                boolean forceToQueue) {
    switch (sourceTag.getSourceTagLiteral()) {
      case "SourceDescription":
        if (sourceTag.getAction().equals("delete")) {
          return wavefrontAPI.removeDescription(sourceTag.getSource(), forceToQueue);
        } else {
          return wavefrontAPI.setDescription(sourceTag.getSource(), sourceTag.getDescription(), forceToQueue);
        }
      case "SourceTag":
        if (sourceTag.getAction().equals("delete")) {
          // call the api, if we receive a 406 message then we add them to the queue
          // TODO: right now it only deletes the first tag (because that server-side api
          // only handles one tag at a time. Once the server-side api is updated we
          // should update this code to remove multiple tags at a time.
          return wavefrontAPI.removeTag(sourceTag.getSource(), sourceTag.getAnnotations().get(0), forceToQueue);

        } else { //
          // call the api, if we receive a 406 message then we add them to the queue
          return wavefrontAPI.setTags(sourceTag.getSource(), sourceTag.getAnnotations(), forceToQueue);
        }
      default:
        logger.warning("None of the literals matched. Expected SourceTag or " +
            "SourceDescription. Input = " + sourceTag);
        return null;
    }
  }
}
