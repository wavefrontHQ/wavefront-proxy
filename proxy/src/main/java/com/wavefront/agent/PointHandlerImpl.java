package com.wavefront.agent;

import com.wavefront.common.Clock;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import wavefront.report.ReportPoint;

import static com.wavefront.agent.Validation.validatePoint;

/**
 * Adds all graphite strings to a working list, and batches them up on a set schedule (100ms) to be sent (through the
 * daemon's logic) up to the collector on the server side.
 */
public class PointHandlerImpl implements PointHandler {

  private static final Logger logger = Logger.getLogger(PointHandlerImpl.class.getCanonicalName());
  private static final Logger blockedPointsLogger = Logger.getLogger("RawBlockedPoints");
  private static final Logger validPointsLogger = Logger.getLogger("RawValidPoints");

  private final Histogram receivedPointLag;
  private final String validationLevel;
  private final String handle;
  private final boolean logPoints;

  @Nullable
  private final String prefix;

  protected final int blockedPointsPerBatch;
  protected final PostPushDataTimedTask[] sendDataTasks;

  public PointHandlerImpl(final String handle,
                          final String validationLevel,
                          final int blockedPointsPerBatch,
                          final PostPushDataTimedTask[] sendDataTasks) {
    this(handle, validationLevel, blockedPointsPerBatch, null, sendDataTasks);
  }

  public PointHandlerImpl(final String handle,
                          final String validationLevel,
                          final int blockedPointsPerBatch,
                          @Nullable final String prefix,
                          final PostPushDataTimedTask[] sendDataTasks) {
    this.validationLevel = validationLevel;
    this.handle = handle;
    this.blockedPointsPerBatch = blockedPointsPerBatch;
    this.prefix = prefix;
    String logPointsProperty = System.getProperty("wavefront.proxy.logpoints");
    this.logPoints = logPointsProperty != null && logPointsProperty.equalsIgnoreCase("true");

    this.receivedPointLag = Metrics.newHistogram(new MetricName("points." + handle + ".received", "", "lag"));

    this.sendDataTasks = sendDataTasks;
  }

  @Override
  public void reportPoint(ReportPoint point, @Nullable String debugLine) {
    final PostPushDataTimedTask randomPostTask = getRandomPostTask();
    try {
      if (prefix != null) {
        point.setMetric(prefix + "." + point.getMetric());
      }
      validatePoint(
          point,
          "" + handle,
          debugLine,
          validationLevel == null ? null : Validation.Level.valueOf(validationLevel));

      // No validation was requested by user; send forward.
      String strPoint = pointToString(point);
      if (logPoints || validPointsLogger.isLoggable(Level.FINEST)) {
        // we log valid points only if system property wavefront.proxy.logpoints is true or RawValidPoints log level is
        // set to "ALL". this is done to prevent introducing overhead and accidentally logging points to the main log
        validPointsLogger.info(strPoint);
      }
      randomPostTask.addPoint(strPoint);
      randomPostTask.enforceBufferLimits();
      receivedPointLag.update(Clock.now() - point.getTimestamp());

    } catch (IllegalArgumentException e) {
      blockedPointsLogger.warning(pointToString(point));
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

  private static String quote = "\"";
  private static String escapedQuote = "\\\"";

  private static String escapeQuotes(String raw) {
    return StringUtils.replace(raw, quote, escapedQuote);
  }

  private static void appendTagMap(StringBuilder sb, @Nullable Map<String, String> tags) {
    if (tags == null) {
      return;
    }
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      sb.append(' ').append(quote).append(escapeQuotes(entry.getKey())).append(quote)
          .append("=")
          .append(quote).append(escapeQuotes(entry.getValue())).append(quote);
    }
  }

  private static String pointToStringSB(ReportPoint point) {
    if (point.getValue() instanceof Double || point.getValue() instanceof Long || point.getValue() instanceof String) {
      StringBuilder sb = new StringBuilder(quote)
          .append(escapeQuotes(point.getMetric())).append(quote).append(" ")
          .append(point.getValue()).append(" ")
          .append(point.getTimestamp() / 1000).append(" ")
          .append("source=").append(quote).append(escapeQuotes(point.getHost())).append(quote);
      appendTagMap(sb, point.getAnnotations());
      return sb.toString();
    } else if (point.getValue() instanceof wavefront.report.Histogram){
      wavefront.report.Histogram h = (wavefront.report.Histogram) point.getValue();

      StringBuilder sb = new StringBuilder();

      // BinType
      switch (h.getDuration()) {
        case (int) DateUtils.MILLIS_PER_MINUTE:
          sb.append("!M ");
          break;
        case (int) DateUtils.MILLIS_PER_HOUR:
          sb.append("!H ");
          break;
        case (int) DateUtils.MILLIS_PER_DAY:
          sb.append("!D ");
          break;
        default:
          throw new RuntimeException("Unexpected histogram duration " + h.getDuration());
      }

      // Timestamp
      sb.append(point.getTimestamp() / 1000).append(' ');

      // Centroids
      int numCentroids = Math.min(CollectionUtils.size(h.getBins()), CollectionUtils.size(h.getCounts()));
      for (int i=0; i<numCentroids; ++i) {
        // Count
        sb.append('#').append(h.getCounts().get(i)).append(' ');
        // Mean
        sb.append(h.getBins().get(i)).append(' ');
      }

      // Metric
      sb.append(quote).append(escapeQuotes(point.getMetric())).append(quote).append(" ");

      // Source
      sb.append("source=").append(quote).append(escapeQuotes(point.getHost())).append(quote);
      appendTagMap(sb, point.getAnnotations());
      return sb.toString();
    }
    throw new RuntimeException("Unsupported value class: " + point.getValue().getClass().getCanonicalName());
  }


  public static String pointToString(ReportPoint point) {
    return pointToStringSB(point);
  }
}
