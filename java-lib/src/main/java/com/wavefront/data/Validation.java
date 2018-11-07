package com.wavefront.data;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.lang.StringUtils;

import java.util.Map;

import javax.annotation.Nullable;

import wavefront.report.Histogram;
import wavefront.report.ReportPoint;

import static com.wavefront.data.Validation.Level.NO_VALIDATION;

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
        if (!((i == 0 && cur == 0x2206) || (i == 0 && cur == 0x0394) || (i == 0 && cur == 126))) {
          // first character can also be \u2206 (∆ - INCREMENT) or \u0394 (Δ - GREEK CAPITAL LETTER DELTA)
          // or ~ tilda character for internal metrics
          return false;
        }
      }
    }
    return true;
  }

  public static boolean annotationKeysAreValid(ReportPoint point) {
    for (String key : point.getAnnotations().keySet()) {
      if (!charactersAreValid(key)) {
        return false;
      }
    }
    return true;
  }

  public static void validatePoint(ReportPoint point, String source, @Nullable Level validationLevel) {
    Object pointValue = point.getValue();

    if (StringUtils.isBlank(point.getHost())) {
      throw new IllegalArgumentException("WF-301: Source/host name is required");

    }
    if (point.getHost().length() >= 1024) {
      throw new IllegalArgumentException("WF-301: Source/host name is too long: " + point.getHost());
    }

    if (point.getMetric().length() >= 1024) {
      throw new IllegalArgumentException("WF-301: Metric name is too long: " + point.getMetric());
    }

    if (!charactersAreValid(point.getMetric())) {
      illegalCharacterPoints.inc();
      throw new IllegalArgumentException("WF-400 " + source + ": Point metric has illegal character");
    }

    if (point.getAnnotations() != null) {
      if (!annotationKeysAreValid(point)) {
        throw new IllegalArgumentException("WF-401 " + source + ": Point annotation key has illegal character");
      }

      // Each tag of the form "k=v" must be < 256
      for (Map.Entry<String, String> tag : point.getAnnotations().entrySet()) {
        if (tag.getKey().length() + tag.getValue().length() >= 255) {
          throw new IllegalArgumentException("Tag too long: " + tag.getKey() + "=" + tag.getValue());
        }
      }
    }

    if ((validationLevel != null) && (!validationLevel.equals(NO_VALIDATION))) {
      // Is it the right type of point?
      switch (validationLevel) {
        case NUMERIC_ONLY:
          if (!(pointValue instanceof Long) && !(pointValue instanceof Double) && !(pointValue instanceof Histogram)) {
            throw new IllegalArgumentException("WF-403 " + source + ": Was not long/double/histogram object");
          }
          if (pointValue instanceof Histogram) {
            Histogram histogram = (Histogram) pointValue;
            if (histogram.getCounts().size() == 0 || histogram.getBins().size() == 0 ||
                histogram.getCounts().stream().allMatch(i -> i == 0)) {
              throw new IllegalArgumentException("WF-405 " + source + ": Empty histogram");
            }
          }
          break;
      }
    }
  }
}
