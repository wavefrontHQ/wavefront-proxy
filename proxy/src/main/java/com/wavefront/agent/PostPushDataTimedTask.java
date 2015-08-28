package com.wavefront.agent;

import com.google.common.util.concurrent.RateLimiter;
import com.wavefront.agent.api.ForceQueueEnabledAgentAPI;
import com.wavefront.api.agent.Constants;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Andrew Kao (andrew@wavefront.com)
 */
public class PostPushDataTimedTask implements Runnable {
  private static final Logger logger = Logger.getLogger(PostPushDataTimedTask.class.getCanonicalName());

  // TODO: enum
  public static final String LOG_NONE = "NONE";
  public static final String LOG_SUMMARY = "SUMMARY";
  public static final String LOG_DETAILED = "DETAILED";

  private static long INTERVALS_PER_SUMMARY = 60;

  private List<String> points = new ArrayList<String>();
  private List<String> blockedSamples = new ArrayList<String>();

  private ReentrantReadWriteLock.WriteLock writeLock;

  private RateLimiter warningMessageRateLimiter = RateLimiter.create(0.2);

  private final Counter pointsReceived;
  private final Counter pointsSent;
  private final Counter pointsQueued;
  private final Counter pointsBlocked;
  private final Timer batchSendTime;

  private long numIntervals = 0;
  private long numApiCalls = 0;

  private UUID daemonId;
  private int port;
  private int pointsPerBatch;
  private String logLevel;

  private ForceQueueEnabledAgentAPI agentAPI;

  public void addPoint(String metricString) {
    writeLock.lock();
    try {
      pointsReceived.inc();
      this.points.add(metricString);
    } finally {
      writeLock.unlock();
    }
  }

  public void addPoints(List<String> metricStrings) {
    writeLock.lock();
    try {
      pointsReceived.inc(metricStrings.size());
      this.points.addAll(metricStrings);
    } finally {
      writeLock.unlock();
    }
  }

  public int getBlockedSampleSize() {
    return blockedSamples.size();
  }

  public void addBlockedSample(String blockedSample) {
    writeLock.lock();
    try {
      blockedSamples.add(blockedSample);
    } finally {
      writeLock.unlock();
    }
  }

  public void incrementBlockedPoints() {
    this.pointsBlocked.inc();
  }

  public long getNumPointsSent() {
    return this.pointsSent.count();
  }

  public long getNumPointsQueued() {
    return this.pointsQueued.count();
  }

  public long getNumApiCalls() {
    return numApiCalls;
  }

  public UUID getDaemonId() {
    return daemonId;
  }

  public PostPushDataTimedTask(ForceQueueEnabledAgentAPI agentAPI, int pointsPerBatch, String logLevel, UUID daemonId, int port) {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    writeLock = lock.writeLock();

    this.pointsPerBatch = pointsPerBatch;
    this.logLevel = logLevel;
    this.daemonId = daemonId;
    this.port = port;

    this.agentAPI = agentAPI;

    this.pointsSent = Metrics.newCounter(new MetricName("points." + String.valueOf(port), "", "sent"));
    this.pointsQueued = Metrics.newCounter(new MetricName("points." + String.valueOf(port), "", "queued"));
    this.pointsBlocked = Metrics.newCounter(new MetricName("points." + String.valueOf(port), "", "blocked"));
    this.pointsReceived = Metrics.newCounter(new MetricName("points." + String.valueOf(port), "", "received"));
    this.batchSendTime = Metrics.newTimer(new MetricName("push." + String.valueOf(port), "", "duration"), TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
  }

  @Override
  public void run() {
    try {
      List<String> current = createAgentPostBatch();

      if (current.size() != 0) {
        TimerContext timerContext = this.batchSendTime.time();
        try {
          Response response = agentAPI.postPushData(daemonId, Constants.GRAPHITE_BLOCK_WORK_UNIT,
              System.currentTimeMillis(), Constants.PUSH_FORMAT_GRAPHITE_V2,
              GraphiteStringHandler.joinPushData(current));
          int pointsInList = current.size();
          this.pointsSent.inc(pointsInList);
          if (response.getStatus() == Response.Status.NOT_ACCEPTABLE.getStatusCode()) {
            this.pointsQueued.inc(pointsInList);
          }
        } finally {
          numApiCalls++;
          timerContext.stop();
        }

        if (points.size() > getQueuedPointLimit()) {
          if (warningMessageRateLimiter.tryAcquire()) {
            logger.warning("too many pending points (" + points.size() + "), block size: " + pointsPerBatch +
                ". flushing to retry queue");
          }

          // there are going to be too many points to be able to flush w/o the agent blowing up
          // drain the leftovers straight to the retry queue (i.e. to disk)
          writeLock.lock();
          // don't let anyone add any more to points while we're draining it.
          try {

            while (points.size() > 0) {
              List<String> pushData = createAgentPostBatch();
              int pushDataPointCount = pushData.size();
              if (pushDataPointCount > 0) {
                agentAPI.postPushData(daemonId, Constants.GRAPHITE_BLOCK_WORK_UNIT,
                    System.currentTimeMillis(), Constants.PUSH_FORMAT_GRAPHITE_V2,
                    GraphiteStringHandler.joinPushData(pushData), true);

                // update the counters as if this was a failed call to the API
                this.pointsSent.inc(pushDataPointCount);
                this.pointsQueued.inc(pushDataPointCount);
                numApiCalls++;
              } else {
                // this is probably unnecessary
                break;
              }
            }
          } finally {
            writeLock.unlock();
          }
        }
      }
    } catch (Throwable t) {
      logger.log(Level.SEVERE, "Unexpected error in flush loop", t);
    }
  }

  private List<String> createAgentPostBatch() {
    List<String> current;
    List<String> currentBlockedSamples;
    int blockSize;
    writeLock.lock();
    try {
      blockSize = Math.min(points.size(), pointsPerBatch);
      current = points.subList(0, blockSize);
      currentBlockedSamples = null;

      numIntervals += 1;

      points = new ArrayList<>(points.subList(blockSize, points.size()));
      if (((numIntervals % INTERVALS_PER_SUMMARY) == 0) && !blockedSamples.isEmpty()) {
        // Copy this to a temp structure that we can iterate over for printing below
        if ((!logLevel.equals(LOG_NONE))) {
          currentBlockedSamples = new ArrayList<>(blockedSamples);
        }
        blockedSamples.clear();
      }
    } finally {
      writeLock.unlock();
    }

    if (logLevel.equals(LOG_DETAILED)) {
      logger.warning(port + " (DETAILED): Will send " + current.size() + " valid points in this interval, with " +
          points.size() +
          " in backlog; in total, have sent " + getNumPointsSent() + " valid, " + this.pointsBlocked.count() + " " +
          "blocked.");
    }
    if (((numIntervals % INTERVALS_PER_SUMMARY) == 0) && (!logLevel.equals(LOG_NONE))) {
      logger.warning(port + " (SUMMARY): Have sent " + getNumPointsSent() + " valid points; blocked " +
          this.pointsBlocked.count() + ".");
      if (currentBlockedSamples != null) {
        for (String blockedLine : currentBlockedSamples) {
          logger.warning(port + ":  Blocked line [" + blockedLine + "]");
        }
      }
    }
    return current;
  }


  private long getQueuedPointLimit() {
    // if there's more than 2 batches worth of points, that's going to be too much
    return pointsPerBatch * Runtime.getRuntime().availableProcessors() * 2;
  }
}
