package com.wavefront.agent;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;

import java.util.Map;

import javax.annotation.Nullable;

import sunnylabs.report.Histogram;
import sunnylabs.report.ReportPoint;

import static com.wavefront.agent.Validation.Level.NO_VALIDATION;

/**
 * Consolidates point validation logic for point handlers
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class Validation {

  public enum Level {
    NO_VALIDATION,
    NUMERIC_ONLY
  }

  private static final long MILLIS_IN_YEAR = DateUtils.MILLIS_PER_DAY * 365;

  private final static Counter outOfRangePointTimes = Metrics.newCounter(new MetricName("point", "", "badtime"));
  private final static Counter illegalCharacterPoints = Metrics.newCounter(new MetricName("point", "", "badchars"));

  /**
   * Validates that the given host value is valid
   *
   * @param host the host to check
   * @throws IllegalArgumentException when host is blank or null
   * @throws IllegalArgumentException when host is > 1024 characters
   */
  private static void validateHost(String host) {
    if (StringUtils.isBlank(host)) {
      throw new IllegalArgumentException("WF-301: Host is required");
    }
    if (host.length() >= 1024) {
      throw new IllegalArgumentException("WF-301: Host is too long: " + host);
    }
  }

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

  static boolean annotationKeysAreValid(ReportPoint point) {
    for (String key : point.getAnnotations().keySet()) {
      if (!charactersAreValid(key)) {
        return false;
      }
    }
    return true;
  }

  static boolean pointInRange(ReportPoint point) {
    long pointTime = point.getTimestamp();
    long rightNow = System.currentTimeMillis();

    // within 1 year ago and 1 day ahead
    return (pointTime > (rightNow - MILLIS_IN_YEAR)) && (pointTime < (rightNow + DateUtils.MILLIS_PER_DAY));
  }


  public static void validatePoint(
      ReportPoint point,
      String source,
      String debugLine,
      @Nullable Level validationLevel) {
    Object pointValue = point.getValue();

    validateHost(point.getHost());

    if (point.getMetric().length() >= 1024) {
      throw new IllegalArgumentException("WF-301: Metric name is too long: " + point.getMetric());
    }

    if (!charactersAreValid(point.getMetric())) {
      illegalCharacterPoints.inc();
      String errorMessage = "WF-400 " + source + ": Point metric has illegal character (" + debugLine + ")";
      throw new IllegalArgumentException(errorMessage);
    }

    if (point.getAnnotations() != null) {
      if (!annotationKeysAreValid(point)) {
        String errorMessage = "WF-401 " + source + ": Point annotation key has illegal character (" + debugLine + ")";
        throw new IllegalArgumentException(errorMessage);
      }

      // Each tag of the form "k=v" must be < 256
      for (Map.Entry<String, String> tag : point.getAnnotations().entrySet()) {
        if (tag.getKey().length() + tag.getValue().length() >= 255) {
          throw new IllegalArgumentException("Tag too long: " + tag.getKey() + "=" + tag.getValue());
        }
      }
    }
    if (!pointInRange(point)) {
      outOfRangePointTimes.inc();
      String errorMessage = "WF-402 " + source + ": Point outside of reasonable time frame (" + debugLine + ")";
      throw new IllegalArgumentException(errorMessage);
    }

    if ((validationLevel != null) && (!validationLevel.equals(NO_VALIDATION))) {
      // Is it the right type of point?
      switch (validationLevel) {
        case NUMERIC_ONLY:
          if (!(pointValue instanceof Long) && !(pointValue instanceof Double) && !(pointValue instanceof Histogram)) {
            String errorMessage = "WF-403 " + source + ": Was not long/double/histogram object (" + debugLine + ")";
            throw new IllegalArgumentException(errorMessage);
          }
          break;
      }
    }
  }
}
