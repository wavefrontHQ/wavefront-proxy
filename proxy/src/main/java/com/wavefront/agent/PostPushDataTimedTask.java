package com.wavefront.agent;

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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.core.Response;

/**
 * @author Andrew Kao (andrew@wavefront.com)
 */
public class PostPushDataTimedTask implements Runnable {

  private static final Logger logger = Logger.getLogger(PostPushDataTimedTask.class.getCanonicalName());

  private static final int MAX_SPLIT_BATCH_SIZE = 50000; // same value as default pushFlushMaxPoints

  private List<String> points = new ArrayList<>();
  private final Object pointsMutex = new Object();
  private final List<String> blockedSamples = new ArrayList<>();

  private final String pushFormat;
  private final Object blockedSamplesMutex = new Object();

  /**
   * Warn about exceeding the rate limit no more than once per 10 seconds (per thread)
   */
  private final RateLimiter warningMessageRateLimiter = RateLimiter.create(0.1);

  /**
   * Print summary once a minute
   */
  private final RateLimiter summaryMessageRateLimiter = RateLimiter.create(0.017);

  /**
   * Write a sample of blocked points to log once a minute
   */
  private final RateLimiter blockedSamplesRateLimiter = RateLimiter.create(0.017);
  private final RecyclableRateLimiter pushRateLimiter;

  private final Counter pointsReceived;
  private final Counter pointsAttempted;
  private final Counter pointsQueued;
  private final Counter pointsBlocked;
  private final Counter permitsGranted;
  private final Counter permitsDenied;
  private final Counter permitsRetried;
  private final Counter batchesAttempted;
  private final Timer batchSendTime;

  private long numApiCalls = 0;

  private UUID daemonId;
  private String handle;
  private final int threadId;
  private long pushFlushInterval;
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private static int pointsPerBatch = MAX_SPLIT_BATCH_SIZE;
  private static int memoryBufferLimit = MAX_SPLIT_BATCH_SIZE * 32;
  private boolean isFlushingToQueue = false;

  private ForceQueueEnabledAgentAPI agentAPI;

  static void setPointsPerBatch(int newSize) {
    pointsPerBatch = Math.min(newSize, MAX_SPLIT_BATCH_SIZE);
    pointsPerBatch = Math.max(pointsPerBatch, 1);
  }

  static void setMemoryBufferLimit(int newSize) {
    memoryBufferLimit = Math.max(newSize, pointsPerBatch);
  }

  public void addPoint(String metricString) {
    pointsReceived.inc();
    synchronized (pointsMutex) {
      this.points.add(metricString);
    }
  }

  public void addPoints(List<String> metricStrings) {
    pointsReceived.inc(metricStrings.size());
    synchronized (pointsMutex) {
      this.points.addAll(metricStrings);
    }
  }

  public int getBlockedSampleSize() {
    synchronized (blockedSamplesMutex) {
      return blockedSamples.size();
    }
  }

  public void addBlockedSample(String blockedSample) {
    synchronized (blockedSamplesMutex) {
      blockedSamples.add(blockedSample);
    }
  }

  public void incrementBlockedPoints() {
    this.pointsBlocked.inc();
  }

  public long getAttemptedPoints() {
    return this.pointsAttempted.count();
  }

  public long getNumPointsQueued() {
    return this.pointsQueued.count();
  }

  public long getNumPointsToSend() {
    return this.points.size();
  }

  public boolean getFlushingToQueueFlag() {
    return isFlushingToQueue;
  }

  public long getNumApiCalls() {
    return numApiCalls;
  }

  public UUID getDaemonId() {
    return daemonId;
  }

  @Deprecated
  public PostPushDataTimedTask(String pushFormat, ForceQueueEnabledAgentAPI agentAPI, String logLevel,
                               UUID daemonId, String handle, int threadId, RecyclableRateLimiter pushRateLimiter,
                               long pushFlushInterval) {
    this(pushFormat, agentAPI, daemonId, handle, threadId, pushRateLimiter, pushFlushInterval);
  }

  public PostPushDataTimedTask(String pushFormat, ForceQueueEnabledAgentAPI agentAPI,
                               UUID daemonId, String handle, int threadId, RecyclableRateLimiter pushRateLimiter,
                               long pushFlushInterval) {
    this.pushFormat = pushFormat;
    this.daemonId = daemonId;
    this.handle = handle;
    this.threadId = threadId;
    this.pushFlushInterval = pushFlushInterval;
    this.agentAPI = agentAPI;
    this.pushRateLimiter = pushRateLimiter;

    this.pointsAttempted = Metrics.newCounter(new MetricName("points." + handle, "", "sent"));
    this.pointsQueued = Metrics.newCounter(new MetricName("points." + handle, "", "queued"));
    this.pointsBlocked = Metrics.newCounter(new MetricName("points." + handle, "", "blocked"));
    this.pointsReceived = Metrics.newCounter(new MetricName("points." + handle, "", "received"));
    this.permitsGranted = Metrics.newCounter(new MetricName("limiter", "", "permits-granted"));
    this.permitsDenied = Metrics.newCounter(new MetricName("limiter", "", "permits-denied"));
    this.permitsRetried = Metrics.newCounter(new MetricName("limiter", "", "permits-retried"));
    this.batchesAttempted = Metrics.newCounter(
        new MetricName("push." + String.valueOf(handle) + ".thread-" + String.valueOf(threadId), "", "batches"));
    this.batchSendTime = Metrics.newTimer(new MetricName("push." + handle, "", "duration"),
        TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
    this.scheduler.schedule(this, pushFlushInterval, TimeUnit.MILLISECONDS);
  }

  @Override
  public void run() {
    long nextRunMillis = this.pushFlushInterval;
    try {
      List<String> current = createAgentPostBatch();
      batchesAttempted.inc();
      if (current.size() == 0) {
        return;
      }
      if (pushRateLimiter == null || pushRateLimiter.tryAcquire(current.size())) {
        if (pushRateLimiter != null) this.permitsGranted.inc(current.size());

        TimerContext timerContext = this.batchSendTime.time();
        Response response = null;
        try {
          response = agentAPI.postPushData(
              daemonId,
              Constants.GRAPHITE_BLOCK_WORK_UNIT,
              System.currentTimeMillis(),
              pushFormat,
              StringLineIngester.joinPushData(current));
          int pointsInList = current.size();
          this.pointsAttempted.inc(pointsInList);
          if (response.getStatus() == Response.Status.NOT_ACCEPTABLE.getStatusCode()) {
            if (pushRateLimiter != null) {
              this.pushRateLimiter.recyclePermits(pointsInList);
              this.permitsRetried.inc(pointsInList);
            }
            this.pointsQueued.inc(pointsInList);
          }
        } finally {
          numApiCalls++;
          timerContext.stop();
          if (response != null) response.close();
        }

        if (points.size() > memoryBufferLimit) {
          // there are going to be too many points to be able to flush w/o the agent blowing up
          // drain the leftovers straight to the retry queue (i.e. to disk)
          // don't let anyone add any more to points while we're draining it.
          logger.warning("[FLUSH THREAD " + threadId + "]: WF-3 Too many pending points (" + points.size() +
              "), block size: " + pointsPerBatch + ". flushing to retry queue");
          drainBuffersToQueue();
          logger.info("[FLUSH THREAD " + threadId + "]: flushing to retry queue complete. " +
              "Pending points: " + points.size());
        }
      } else {
        this.permitsDenied.inc(current.size());
        // if proxy rate limit exceeded, try again in 250..500ms (to introduce some degree of fairness)
        nextRunMillis = 250 + (int) (Math.random() * 250);
        if (warningMessageRateLimiter.tryAcquire()) {
          logger.warning("[FLUSH THREAD " + threadId + "]: WF-4 Proxy rate limit exceeded " +
              "(pending points: " + points.size() + "), will retry");
        }
        synchronized (pointsMutex) { // return the batch to the beginning of the queue
          points.addAll(0, current);
        }
      }
    } catch (Throwable t) {
      logger.log(Level.SEVERE, "Unexpected error in flush loop", t);
    } finally {
      scheduler.schedule(this, nextRunMillis, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Shut down the scheduler for this task (prevent future scheduled runs)
   */
  public void shutdown() {
    scheduler.shutdown();
  }

  public void drainBuffersToQueue() {
    try {
      isFlushingToQueue = true;
      // roughly limit number of points to flush to the the current buffer size (+1 blockSize max)
      // if too many points arrive at the proxy while it's draining, they will be taken care of in the next run
      int pointsToFlush = points.size();
      while (pointsToFlush > 0) {
        List<String> pushData = createAgentPostBatch();
        int pushDataPointCount = pushData.size();
        if (pushDataPointCount > 0) {
          agentAPI.postPushData(daemonId, Constants.GRAPHITE_BLOCK_WORK_UNIT,
              System.currentTimeMillis(), Constants.PUSH_FORMAT_GRAPHITE_V2,
              StringLineIngester.joinPushData(pushData), true);

          // update the counters as if this was a failed call to the API
          this.pointsAttempted.inc(pushDataPointCount);
          this.pointsQueued.inc(pushDataPointCount);
          this.permitsDenied.inc(pushDataPointCount);
          numApiCalls++;
          pointsToFlush -= pushDataPointCount;
        } else {
          break;
        }
      }
    } finally {
      isFlushingToQueue = false;
    }
  }

  private void logBlockedPoints() {
    if (blockedSamplesRateLimiter.tryAcquire()) {
      List<String> currentBlockedSamples = new ArrayList<>();
      if (!blockedSamples.isEmpty()) {
        synchronized (blockedSamplesMutex) {
          // Copy this to a temp structure that we can iterate over for printing below
          currentBlockedSamples.addAll(blockedSamples);
          blockedSamples.clear();
        }
      }
      for (String blockedLine : currentBlockedSamples) {
        logger.info("[" + handle + "] blocked input: [" + blockedLine + "]");
      }
    }
  }

  private List<String> createAgentPostBatch() {
    List<String> current;
    int blockSize;
    synchronized (pointsMutex) {
      blockSize = Math.min(points.size(), pointsPerBatch);
      current = points.subList(0, blockSize);
      points = new ArrayList<>(points.subList(blockSize, points.size()));
    }
    if (summaryMessageRateLimiter.tryAcquire()) {
      logger.info("[" + handle + "] (SUMMARY): points attempted: " + getAttemptedPoints() +
          "; blocked: " + this.pointsBlocked.count());
    }
    logBlockedPoints();
    logger.fine("[" + handle + "] (DETAILED): sending " + current.size() + " valid points" +
        "; points in memory: " + points.size() +
        "; total attempted points: " + getAttemptedPoints() +
        "; total blocked: " + this.pointsBlocked.count() +
        "; total queued: " + getNumPointsQueued());
    return current;
  }
}
