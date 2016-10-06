package com.wavefront.agent;

import com.google.common.annotations.VisibleForTesting;

import com.wavefront.common.Clock;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import sunnylabs.report.ReportPoint;

/**
 * Adds all graphite strings to a working list, and batches them up on a set schedule (100ms) to be sent (through the
 * daemon's logic) up to the collector on the server side.
 */
public class PointHandlerImpl implements PointHandler {

  private static final Logger logger = Logger.getLogger(PointHandlerImpl.class.getCanonicalName());
  private static final Random random = new Random();

  // What types of data should be validated and sent to the cloud?
  public static final String VALIDATION_NO_VALIDATION = "NO_VALIDATION";  // Validate nothing
  public static final String VALIDATION_NUMERIC_ONLY = "NUMERIC_ONLY";    // Validate/send numerics; block text

  private final Counter illegalCharacterPoints;
  private final Histogram receivedPointLag;
  private final String validationLevel;
  private final int port;

  protected final int blockedPointsPerBatch;
  protected final PostPushDataTimedTask[] sendDataTasks;

  public PointHandlerImpl(final int port,
                          final String validationLevel,
                          final int blockedPointsPerBatch,
                          final PostPushDataTimedTask[] sendDataTasks) {
    this.validationLevel = validationLevel;
    this.port = port;
    this.blockedPointsPerBatch = blockedPointsPerBatch;

    this.illegalCharacterPoints = Metrics.newCounter(new MetricName("point", "", "badchars"));
    this.receivedPointLag = Metrics.newHistogram(
        new MetricName("points." + String.valueOf(port) + ".received", "", "lag"));

    this.sendDataTasks = sendDataTasks;
  }

  @Override
  public void reportPoint(ReportPoint point, @Nullable String debugLine) {
    final PostPushDataTimedTask randomPostTask = getRandomPostTask();
    try {
      Object pointValue = point.getValue();

      validateHost(point.getHost());

      if (point.getMetric().length() >= 1024) {
        throw new IllegalArgumentException("WF-301: Metric name is too long: " + point.getMetric());
      }

      if (!charactersAreValid(point.getMetric())) {
        illegalCharacterPoints.inc();
        String errorMessage = "WF-400 " + port + ": Point metric has illegal character (" +
            (debugLine == null ? pointToString(point) : debugLine) + ")";
        throw new IllegalArgumentException(errorMessage);
      }

      if (!annotationKeysAreValid(point)) {
        String errorMessage = "WF-401 " + port + ": Point annotation key has illegal character (" +
            (debugLine == null ? pointToString(point) : debugLine) + ")";
        throw new IllegalArgumentException(errorMessage);
      }

      // Each tag of the form "k=v" must be < 256
      for (Map.Entry<String, String> tag : point.getAnnotations().entrySet()) {
        if (tag.getKey().length() + tag.getValue().length() >= 255) {
          throw new IllegalArgumentException("Tag too long: " + tag.getKey() + "=" + tag.getValue() + "(" +
              (debugLine == null ? pointToString(point) : debugLine) + ")");
        }
      }
      if ((validationLevel != null) && (!validationLevel.equals(VALIDATION_NO_VALIDATION))) {
        // Is it the right type of point?
        switch (validationLevel) {
          case VALIDATION_NUMERIC_ONLY:
            if (!(pointValue instanceof Long) && !(pointValue instanceof Double)) {
              String errorMessage = "WF-403 " + port + ": Was not long/double object (" +
                  (debugLine == null ? pointToString(point) : debugLine) + ")";
              throw new IllegalArgumentException(errorMessage);
            }
            break;
        }
        randomPostTask.addPoint(pointToString(point));
        receivedPointLag.update(Clock.now() - point.getTimestamp());
      } else {
        // No validation was requested by user; send forward.
        randomPostTask.addPoint(pointToString(point));
        receivedPointLag.update(Clock.now() - point.getTimestamp());
      }
    } catch (IllegalArgumentException e) {
      this.handleBlockedPoint(e.getMessage());
    } catch (Exception ex) {
      logger.log(Level.SEVERE, "WF-500 Uncaught exception when handling point (" +
          (debugLine == null ? pointToString(point) : debugLine) + ")", ex);
    }
  }

  @Override
  public void reportPoints(List<ReportPoint> points) {
    for (final ReportPoint point : points) {
      reportPoint(point, null);
    }
  }

  public PostPushDataTimedTask getRandomPostTask() {
    // return the task with the lowest number of pending points and, if possible, not currently flushing to retry queue
    long min = Long.MAX_VALUE;
    PostPushDataTimedTask randomPostTask = null;
    PostPushDataTimedTask firstChoicePostTask = null;
    for (int i = 0; i < this.sendDataTasks.length; i++) {
      long pointsToSend = this.sendDataTasks[i].getNumPointsToSend();
      if (pointsToSend < min) {
        min = pointsToSend;
        randomPostTask = this.sendDataTasks[i];
        if (!this.sendDataTasks[i].getFlushingToQueueFlag()) {
          firstChoicePostTask = this.sendDataTasks[i];
        }
      }
    }
    return firstChoicePostTask == null ? randomPostTask : firstChoicePostTask;
  }

  @Override
  public void handleBlockedPoint(@Nullable String pointLine) {
    final PostPushDataTimedTask randomPostTask = getRandomPostTask();
    if (pointLine != null && randomPostTask.getBlockedSampleSize() < this.blockedPointsPerBatch) {
      randomPostTask.addBlockedSample(pointLine);
    }
    randomPostTask.incrementBlockedPoints();
  }

  @VisibleForTesting
  static boolean annotationKeysAreValid(ReportPoint point) {
    for (String key : point.getAnnotations().keySet()) {
      if (!charactersAreValid(key)) {
        return false;
      }
    }
    return true;
  }

  @VisibleForTesting
  static boolean charactersAreValid(String input) {
    // Legal characters are 44-57 (,-./ and numbers), 65-90 (upper), 97-122 (lower), 95 (_)
    int l = input.length();
    if (l == 0) {
      return false;
    }

    for (int i = 0; i < l; i++) {
      char cur = input.charAt(i);
      if (!(44 <= cur && cur <= 57) && !(65 <= cur && cur <= 90) && !(97 <= cur && cur <= 122) &&
          cur != 95) {
        if (i != 0 || cur != 126) {
          // first character can be 126 (~)
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Validates that the given host value is valid
   *
   * @param host the host to check
   * @throws IllegalArgumentException when host is blank or null
   * @throws IllegalArgumentException when host is > 1024 characters
   */
  static void validateHost(String host) {
    if (StringUtils.isBlank(host)) {
      throw new IllegalArgumentException("WF-301: Host is required");
    }
    if (host.length() >= 1024) {
      throw new IllegalArgumentException("WF-301: Host is too long: " + host);
    }

  }

  private static String pointToStringSB(ReportPoint point) {
    StringBuilder sb = new StringBuilder("\"")
        .append(point.getMetric().replace("\"", "\\\"")).append("\" ")
        .append(point.getValue()).append(" ")
        .append(point.getTimestamp() / 1000).append(" ")
        .append("source=\"").append(point.getHost().replace("\"", "\\\"")).append("\"");
    for (Map.Entry<String, String> entry : point.getAnnotations().entrySet()) {
      sb.append(" \"").append(entry.getKey().replace("\"", "\\\"")).append("\"")
          .append("=")
          .append("\"").append(entry.getValue().replace("\"", "\\\"")).append("\"");
    }
    return sb.toString();
  }


  @VisibleForTesting
  static String pointToString(ReportPoint point) {
    return pointToStringSB(point);
  }
}
