package com.wavefront.agent;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.agent.api.ForceQueueEnabledAgentAPI;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import org.apache.commons.lang.time.DateUtils;
import sunnylabs.report.ReportPoint;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Adds all graphite strings to a working list, and batches them up on a set schedule (100ms) to
 * be sent (through the daemon's logic) up to the collector on the server side.
 */
public class PointHandler {

  private static final Logger logger = Logger.getLogger(PointHandler.class.getCanonicalName());

  // What types of data should be validated and sent to the cloud?
  public static final String VALIDATION_NO_VALIDATION = "NO_VALIDATION";  // Validate nothing
  public static final String VALIDATION_NUMERIC_ONLY = "NUMERIC_ONLY";    // Validate/send numerics; block text

  private final Counter outOfRangePointTimes;
  private final String validationLevel;
  private final int port;

  protected final int blockedPointsPerBatch;
  protected final PostPushDataTimedTask sendDataTask;

  public PointHandler(final ForceQueueEnabledAgentAPI agentAPI,
                      final UUID daemonId,
                      final int port,
                      final String logLevel,
                      final String validationLevel,
                      final long millisecondsPerBatch,
                      final int pointsPerBatch,
                      final int blockedPointsPerBatch) {
    this.validationLevel = validationLevel;
    this.port = port;
    this.blockedPointsPerBatch = blockedPointsPerBatch;

    this.sendDataTask = new PostPushDataTimedTask(agentAPI, pointsPerBatch, logLevel, daemonId, port);

    this.outOfRangePointTimes = Metrics.newCounter(new MetricName("point", "", "badtime"));

    int numTimerThreadsUsed = Runtime.getRuntime().availableProcessors();
    logger.info("Using " + numTimerThreadsUsed + " timer threads for listener on port: " + port);
    ScheduledExecutorService es = Executors.newScheduledThreadPool(numTimerThreadsUsed);
    for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
      es.scheduleWithFixedDelay(this.sendDataTask, millisecondsPerBatch, millisecondsPerBatch, TimeUnit.MILLISECONDS);
    }
  }

  public void reportPoint(ReportPoint point, String pointLine) {
    try {
      Object pointValue = point.getValue();

      if (!pointInRange(point)) {
        outOfRangePointTimes.inc();
        String errorMessage = port + ": Point outside of reasonable time frame (" + pointLine + ")";
        logger.warning(errorMessage);
        throw new RuntimeException(errorMessage);
      }

      if ((validationLevel != null) && (!validationLevel.equals(VALIDATION_NO_VALIDATION))) {
        // Is it the right type of point?
        switch (validationLevel) {
          case VALIDATION_NUMERIC_ONLY:
            if (!(pointValue instanceof Long) && !(pointValue instanceof Double)) {
              throw new RuntimeException(port + ": Was not long/double object");
            }
            break;
        }
        this.sendDataTask.addPoint(pointLine);
      } else {
        // No validation was requested by user; send forward.
        this.sendDataTask.addPoint(pointLine);
      }

    } catch (Exception e) {
      if (this.sendDataTask.getBlockedSampleSize() < this.blockedPointsPerBatch) {
        this.sendDataTask.addBlockedSample(pointLine);
      }
      this.sendDataTask.incrementBlockedPoints();
    }
  }

  private static final long MILLIS_IN_YEAR = DateUtils.MILLIS_PER_DAY * 365;

  @VisibleForTesting
  static boolean pointInRange(ReportPoint point) {
    long pointTime = point.getTimestamp();
    long rightNow = System.currentTimeMillis();

    // within 1 year ago and 1 day ahead
    return (pointTime > (rightNow - MILLIS_IN_YEAR)) && (pointTime < (rightNow + DateUtils.MILLIS_PER_DAY));
  }

  protected String pointToString(ReportPoint point) {
    String toReturn = String.format("\"%s\" %s %d source=\"%s\"",
        point.getMetric().replaceAll("\"", "\\\""),
        point.getValue(),
        point.getTimestamp() / 1000,
        point.getHost().replaceAll("\"", "\\\""));
    for (Map.Entry<String, String> entry : point.getAnnotations().entrySet()) {
      toReturn += String.format(" \"%s\"=\"%s\"",
          entry.getKey().replaceAll("\"", "\\\""),
          entry.getValue().replaceAll("\"", "\\\""));
    }
    return toReturn;
  }
}
