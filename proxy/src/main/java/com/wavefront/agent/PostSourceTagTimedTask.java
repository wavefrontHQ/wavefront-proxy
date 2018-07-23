package com.wavefront.agent;

import com.google.common.util.concurrent.RateLimiter;

import com.wavefront.agent.api.ForceQueueEnabledAgentAPI;
import com.wavefront.common.NamedThreadFactory;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.core.Response;

import wavefront.report.ReportSourceTag;

/**
 * This class is responsible for accumulating the source tag changes and post it in a batch. This
 * class is similar to PostPushDataTimedTask.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com)
 */
@Deprecated
public class PostSourceTagTimedTask implements Runnable {

  private static final Logger logger =
      Logger.getLogger(PostSourceTagTimedTask.class.getCanonicalName());

  private static final int MAX_BATCH_SIZE = 1000;
  private static long INTERVALS_PER_SUMMARY = 60;
  private static int pointsPerBatch = MAX_BATCH_SIZE;

  // TODO: convert it to enum
  public static final String LOG_NONE = "NONE";
  public static final String LOG_SUMMARY = "SUMMARY";
  public static final String LOG_DETAILED = "DETAILED";

  private final ScheduledExecutorService scheduler;
  private final String token;

  // TODO: refactor the queue so that other object types can be handled
  private List<ReportSourceTag> sourceTags = new ArrayList<>();
  private final Object sourceTagMutex = new Object();
  private final List<ReportSourceTag> blockedSamples = new ArrayList<>();
  private final Object blockedSamplesMutex = new Object();

  private final RateLimiter warningMessageRateLimiter = RateLimiter.create(0.2);

  private final Counter sourceTagsReceived;
  private final Counter sourceTagsAttempted;
  private final Counter sourceTagsQueued;
  private final Counter sourceTagsBlocked;
  private final Counter batchesSent;
  private final Timer batchSendTime;

  private static int dataPerBatch = MAX_BATCH_SIZE;
  private long numIntervals = 0;
  private long numApiCalls = 0;
  private String logLevel;
  private int port;
  private ForceQueueEnabledAgentAPI agentAPI;
  private int threadId;
  private boolean isFlushingToQueue = false;
  private final long pushFlushInterval;

  public PostSourceTagTimedTask(ForceQueueEnabledAgentAPI agentAPI, String logLevel, int port,
                                int threadId, long pushFlushInterval, String token) {
    this.agentAPI = agentAPI;
    this.port = port;
    this.threadId = threadId;
    this.logLevel = logLevel;
    this.scheduler = Executors.newScheduledThreadPool(1, new NamedThreadFactory
        ("submitter-sourcetag-" + port + "-" + String.valueOf(threadId)));
    this.sourceTagsAttempted = Metrics.newCounter(new MetricName("sourceTags." + String.valueOf
        (port), "", "sent"));
    this.sourceTagsQueued = Metrics.newCounter(new MetricName("sourceTags." + String.valueOf(port),
        "", "queued"));
    this.sourceTagsBlocked = Metrics.newCounter(new MetricName("sourceTags." + String.valueOf
        (port), "", "blocked"));
    this.sourceTagsReceived = Metrics.newCounter(new MetricName("sourceTags." + String.valueOf
        (port), "", "received"));
    this.batchesSent = Metrics.newCounter(
        new MetricName("pushSourceTag." + String.valueOf(port) + ".thread-" + String.valueOf
            (threadId), "", "batches"));
    this.batchSendTime = Metrics.newTimer(new MetricName("pushSourceTag." + String.valueOf(port),
        "", "duration"), TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
    this.pushFlushInterval = pushFlushInterval;
    this.token = token;
    this.scheduler.schedule(this, this.pushFlushInterval, TimeUnit.MILLISECONDS);
  }

  @Override
  public void run() {
    try {
      List<ReportSourceTag> current = createAgentPostBatch();
      batchesSent.inc();
      if (current.size() != 0) {
        TimerContext timerContext = this.batchSendTime.time();
        Response response = null;
        try {
          boolean forceToQueue = false;
          for (ReportSourceTag sourceTag : current) {
            switch (sourceTag.getSourceTagLiteral()) {
              case "SourceDescription":
                if (sourceTag.getAction().equals("delete")) {
                  response = agentAPI.removeDescription(sourceTag.getSource(), forceToQueue);
                } else {
                  response = agentAPI.setDescription(sourceTag.getSource(), sourceTag.getDescription(), forceToQueue);
                }
                break;
              case "SourceTag":
                if (sourceTag.getAction().equals("delete")) {
                  // call the api, if we receive a 406 message then we add them to the queue
                  // TODO: right now it only deletes the first tag (because that server-side api
                  // only handles one tag at a time. Once the server-side api is updated we
                  // should update this code to remove multiple tags at a time.
                  response = agentAPI.removeTag(sourceTag.getSource(), sourceTag.getAnnotations().get(0), forceToQueue);

                } else { //
                  // call the api, if we receive a 406 message then we add them to the queue
                  response = agentAPI.setTags(sourceTag.getSource(), sourceTag.getAnnotations(), forceToQueue);
                }
                break;
              default:
                logger.warning("None of the literals matched. Expected SourceTag or " +
                    "SourceDescription. Input = " + sourceTag);
            }
            this.sourceTagsAttempted.inc();
            if (response != null && response.getStatus() == Response.Status.NOT_ACCEPTABLE.getStatusCode()) {
              this.sourceTagsQueued.inc();
              forceToQueue = true;
            }
            numApiCalls++;
          }
        } finally {
          timerContext.stop();
          if (response != null) response.close();
        }

        if (sourceTags.size() > getQueuedPointLimit()) {
          if (warningMessageRateLimiter.tryAcquire()) {
            logger.warning("WF-3 Too many pending points (" + sourceTags.size() + "), block " +
                "size: " + pointsPerBatch + ". flushing to retry queue");
          }

          drainBuffersToQueue();
        }
      }
    } catch (Throwable t) {
      logger.log(Level.SEVERE, "Unexpected error in flush loop", t);
    } finally {
      scheduler.schedule(this, this.pushFlushInterval, TimeUnit.MILLISECONDS);
    }
  }

  public void drainBuffersToQueue() {
    try {
      isFlushingToQueue = true;
      while (sourceTags.size() > 0) {
        List<ReportSourceTag> tags = createAgentPostBatch();
        int dataCount = tags.size();
        if (dataCount > 0) {
          for (ReportSourceTag sourceTag : tags) {
            switch (sourceTag.getSourceTagLiteral()) {
              case "SourceDescription":
                if (sourceTag.getAction().equals("delete")) {
                  agentAPI.removeDescription(sourceTag.getSource(), true);
                } else {
                  agentAPI.setDescription(sourceTag.getSource(), sourceTag.getDescription(), true);
                }
                break;
              case "SourceTag":
                if (sourceTag.getAction().equals("delete")) {
                  // delete the source tag
                  agentAPI.removeTag(sourceTag.getSource(), sourceTag.getAnnotations().get(0),
                      true);
                } else {
                  // add the source tag
                  agentAPI.setTags(sourceTag.getSource(), sourceTag.getAnnotations(), true);
                }
                break;
            }
            this.sourceTagsAttempted.inc();
            this.sourceTagsQueued.inc();
            numApiCalls++;
          }
        }
      }
    } finally {
      isFlushingToQueue = true;
    }
  }

  public void addSourceTag(ReportSourceTag sourceTag) {
    sourceTagsReceived.inc();
    synchronized (sourceTagMutex) {
      this.sourceTags.add(sourceTag);
    }
  }

  public long getNumDataToSend() {
    return this.sourceTags.size();
  }

  public boolean getFlushingToQueueFlag() {
    return isFlushingToQueue;
  }

  private long getQueuedPointLimit() {
    // if there's more than 2 batches worth of points, that's going to be too much
    return pointsPerBatch * Runtime.getRuntime().availableProcessors() * 2;
  }

  public long getAttemptedSourceTags() {
    return this.sourceTagsAttempted.count();
  }

  private List<ReportSourceTag> createAgentPostBatch() {
    List<ReportSourceTag> current;
    List<ReportSourceTag> currentBlockedSamples = null;
    int blockSize;
    synchronized (sourceTagMutex) {
      blockSize = Math.min(sourceTags.size(), dataPerBatch);
      current = sourceTags.subList(0, blockSize);

      numIntervals += 1;
      sourceTags = new ArrayList<>(sourceTags.subList(blockSize, sourceTags.size()));
    }
    if (((numIntervals % INTERVALS_PER_SUMMARY) == 0) && !blockedSamples.isEmpty()) {
      synchronized (blockedSamplesMutex) {
        // Copy this to a temp structure that we can iterate over for printing below
        if ((!logLevel.equals(LOG_NONE))) {
          currentBlockedSamples = new ArrayList<>(blockedSamples);
        }
        blockedSamples.clear();
      }
    }
    if (logLevel.equals(LOG_DETAILED)) {
      logger.warning("[" + port + "] (DETAILED): sending " + current.size() + " valid " +
          "sourceTags;" + " queue size:" + sourceTags.size() + "; total attempted sourceTags: " +
          getAttemptedSourceTags() + "; total blocked: " + this.sourceTagsBlocked.count());
    }
    if (((numIntervals % INTERVALS_PER_SUMMARY) == 0) && (!logLevel.equals(LOG_NONE))) {
      logger.warning("[" + port + "] (SUMMARY): sourceTags attempted: " +
          getAttemptedSourceTags() + "; blocked: " + this.sourceTagsBlocked.count());
      if (currentBlockedSamples != null) {
        for (ReportSourceTag blockedLine : currentBlockedSamples) {
          logger.warning("[" + port + "] blocked input: [" + blockedLine.toString() + "]");
        }
      }
    }
    return current;
  }
}
