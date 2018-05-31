package com.wavefront.agent;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;

import java.util.Map;

import javax.annotation.Nullable;

import wavefront.report.Histogram;
import wavefront.report.ReportPoint;

import static com.wavefront.agent.PointHandlerImpl.pointToString;
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

  private final static Counter illegalCharacterPoints = Metrics.newCounter(new MetricName("point", "", "badchars"));

  public static boolean charactersAreValid(String input) {
    // Legal characters are 44-57 (,-./ and numbers), 65-90 (upper), 97-122 (lower), 95 (_)
    int l = input.length();
    if (l == 0) {
      return false;
    }

    for (int i = 0; i < l; i++) {
      char cur = input.charAt(i);
      if (!(44 <= cur && cur <= 57) && !(65 <= cur && cur <= 90) && !(97 <= cur && cur <= 122) &&
          cur != 95) {
        if (!((i == 0 && cur == 0x2206) || (i == 0 && cur == 0x0394))) {
          // first character can also be \u2206 (∆ - INCREMENT) or \u0394 (Δ - GREEK CAPITAL LETTER DELTA)
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

  public static void validatePoint(
      ReportPoint point,
      String source,
      String debugLine,
      @Nullable Level validationLevel) {
    Object pointValue = point.getValue();

    if (StringUtils.isBlank(point.getHost())) {
      String errorMessage = "WF-301: Source/host name is required (" +
          (debugLine == null ? pointToString(point) : debugLine) + ")";
      throw new IllegalArgumentException(errorMessage);

    }
    if (point.getHost().length() >= 1024) {
      String errorMessage = "WF-301: Source/host name is too long: " + point.getHost() + "(" +
          (debugLine == null ? pointToString(point) : debugLine) + ")";
      throw new IllegalArgumentException(errorMessage);
    }

    if (point.getMetric().length() >= 1024) {
      String errorMessage = "WF-301: Metric name is too long: " + point.getMetric() +
          " (" + (debugLine == null ? pointToString(point) : debugLine) + ")";
      throw new IllegalArgumentException(errorMessage);
    }

    if (!charactersAreValid(point.getMetric())) {
      illegalCharacterPoints.inc();
      String errorMessage = "WF-400 " + source + ": Point metric has illegal character (" +
          (debugLine == null ? pointToString(point) : debugLine) + ")";
      throw new IllegalArgumentException(errorMessage);
    }

    if (point.getAnnotations() != null) {
      if (!annotationKeysAreValid(point)) {
        String errorMessage = "WF-401 " + source + ": Point annotation key has illegal character (" +
            (debugLine == null ? pointToString(point) : debugLine) + ")";
        throw new IllegalArgumentException(errorMessage);
      }

      // Each tag of the form "k=v" must be < 256
      for (Map.Entry<String, String> tag : point.getAnnotations().entrySet()) {
        if (tag.getKey().length() + tag.getValue().length() >= 255) {
          String errorMessage = "Tag too long: " + tag.getKey() + "=" + tag.getValue() + " (" +
              (debugLine == null ? pointToString(point) : debugLine) + ")";
          throw new IllegalArgumentException(errorMessage);
        }
      }
    }

    if ((validationLevel != null) && (!validationLevel.equals(NO_VALIDATION))) {
      // Is it the right type of point?
      switch (validationLevel) {
        case NUMERIC_ONLY:
          if (!(pointValue instanceof Long) && !(pointValue instanceof Double) && !(pointValue instanceof Histogram)) {
            String errorMessage = "WF-403 " + source + ": Was not long/double/histogram object (" +
                (debugLine == null ? pointToString(point) : debugLine) + ")";
            throw new IllegalArgumentException(errorMessage);
          }
          break;
      }
    }
  }
}
