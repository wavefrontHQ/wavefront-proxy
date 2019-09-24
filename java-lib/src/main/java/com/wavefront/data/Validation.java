package com.wavefront.data;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.wavefront.api.agent.ValidationConfiguration;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import wavefront.report.Annotation;
import wavefront.report.Histogram;
import wavefront.report.ReportPoint;
import wavefront.report.Span;

import static com.wavefront.data.Validation.Level.NO_VALIDATION;

/**
 * Consolidates point validation logic for point handlers
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
@SuppressWarnings("ConstantConditions")
public class Validation {

  public enum Level {
    NO_VALIDATION,
    NUMERIC_ONLY
  }

  private final static LoadingCache<String, Counter> ERROR_COUNTERS = Caffeine.newBuilder().
      build(x -> Metrics.newCounter(new MetricName("point", "", x)));
  private final static LoadingCache<String, Counter> TRIM_COUNTERS = Caffeine.newBuilder().
      build(x -> Metrics.newCounter(new MetricName("point", "", x)));
  private static final RateLimiter blockedLoggingRateLimiter = RateLimiter.create(1);
  private static final Logger log = Logger.getLogger("DataIngester");

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

  @VisibleForTesting
  static boolean annotationKeysAreValid(ReportPoint point) {
    for (String key : point.getAnnotations().keySet()) {
      if (!charactersAreValid(key)) {
        return false;
      }
    }
    return true;
  }

  public static void validatePoint(ReportPoint point, @Nullable ValidationConfiguration config) {
    if (config == null) {
      return;
    }
    final String host = point.getHost();
    final String metric = point.getMetric();

    Object value = point.getValue();
    boolean isHistogram = value instanceof Histogram;

    if (StringUtils.isBlank(host)) {
      ERROR_COUNTERS.get("sourceMissing").inc();
      throw new IllegalArgumentException("WF-406: Source/host name is required");
    }
    if (host.length() > config.getHostLengthLimit()) {
      ERROR_COUNTERS.get("sourceTooLong").inc();
      throw new IllegalArgumentException("WF-407: Source/host name is too long (" + host.length() +
          " characters, max: " + config.getHostLengthLimit() + "): " + host);
    }
    if (isHistogram) {
      if (metric.length() > config.getHistogramLengthLimit()) {
        ERROR_COUNTERS.get("histogramNameTooLong").inc();
        throw new IllegalArgumentException("WF-409: Histogram name is too long (" + metric.length() +
            " characters, max: " + config.getHistogramLengthLimit() + "): " + metric);
      }
    } else {
      if (metric.length() > config.getMetricLengthLimit()) {
        ERROR_COUNTERS.get("metricNameTooLong").inc();
        throw new IllegalArgumentException("WF-408: Metric name is too long (" + metric.length() +
            " characters, max: " + config.getMetricLengthLimit() + "): " + metric);
      }
    }
    if (!charactersAreValid(metric)) {
      ERROR_COUNTERS.get("badchars").inc();
      throw new IllegalArgumentException("WF-400: Point metric has illegal character(s): " + metric);
    }
    final Map<String, String> annotations = point.getAnnotations();
    if (annotations != null) {
      if (annotations.size() > config.getAnnotationsCountLimit()) {
        ERROR_COUNTERS.get("tooManyPointTags").inc();
        throw new IllegalArgumentException("WF-410: Too many point tags (" + annotations.size() + ", max " +
            config.getAnnotationsCountLimit() + "): ");
      }
      for (Map.Entry<String, String> tag : annotations.entrySet()) {
        final String tagK = tag.getKey();
        final String tagV = tag.getValue();
        // Each tag of the form "k=v" must be < 256
        if (tagK.length() + tagV.length() >= 255) {
          ERROR_COUNTERS.get("pointTagTooLong").inc();
          throw new IllegalArgumentException("WF-411: Point tag (key+value) too long (" + (tagK.length() +
              tagV.length() + 1) + " characters, max: 255): " + tagK + "=" + tagV);
        }
        if (tagK.length() > config.getAnnotationsKeyLengthLimit()) {
          ERROR_COUNTERS.get("pointTagKeyTooLong").inc();
          throw new IllegalArgumentException("WF-412: Point tag key is too long (" + tagK.length() +
              " characters, max: " + config.getAnnotationsKeyLengthLimit() + "): " + tagK);
        }
        if (!charactersAreValid(tagK)) {
          ERROR_COUNTERS.get("badchars").inc();
          throw new IllegalArgumentException("WF-401: Point tag key has illegal character(s): " + tagK);
        }
        if (tagV.length() > config.getAnnotationsValueLengthLimit()) {
          ERROR_COUNTERS.get("pointTagValueTooLong").inc();
          throw new IllegalArgumentException("WF-413: Point tag value is too long (" + tagV.length() +
              " characters, max: " + config.getAnnotationsValueLengthLimit() + "): " + tagV);
        }
      }
    }
    if (!(value instanceof Double || value instanceof Long || value instanceof Histogram)) {
      throw new IllegalArgumentException("WF-403: Value is not a long/double/histogram object: " + value);
    }
    if (value instanceof Histogram) {
      Histogram histogram = (Histogram) value;
      if (histogram.getCounts().size() == 0 || histogram.getBins().size() == 0 ||
          histogram.getCounts().stream().allMatch(i -> i == 0)) {
        throw new IllegalArgumentException("WF-405: Empty histogram");
      }
    } else if ((metric.charAt(0) == 0x2206 || metric.charAt(0) == 0x0394) && ((Number) value).doubleValue() <= 0) {
      throw new IllegalArgumentException("WF-404: Delta metrics cannot be non-positive");
    }
  }

  public static void validateSpan(Span span, @Nullable ValidationConfiguration config) {
    if (config == null) {
      return;
    }
    final String source = span.getSource();
    final String spanName = span.getName();

    if (StringUtils.isBlank(source)) {
      ERROR_COUNTERS.get("spanSourceMissing").inc();
      throw new IllegalArgumentException("WF-426: Span source/host name is required");
    }
    if (source.length() > config.getHostLengthLimit()) {
      ERROR_COUNTERS.get("spanSourceTooLong").inc();
      throw new IllegalArgumentException("WF-427: Span source/host name is too long (" + source.length() +
          " characters, max: " + config.getHostLengthLimit() + "): " + source);
    }
    if (spanName.length() > config.getSpanLengthLimit()) {
      ERROR_COUNTERS.get("spanNameTooLong").inc();
      throw new IllegalArgumentException("WF-428: Span name is too long (" + source.length() + " characters, max: " +
          config.getSpanLengthLimit() + "): " + spanName);
    }
    if (!charactersAreValid(spanName)) {
      ERROR_COUNTERS.get("spanNameBadChars").inc();
      throw new IllegalArgumentException("WF-415: Span name has illegal character(s): " + spanName);
    }
    final List<Annotation> annotations = span.getAnnotations();
    if (annotations != null) {
      if (annotations.size() > config.getSpanAnnotationsCountLimit()) {
        ERROR_COUNTERS.get("spanTooManyAnnotations").inc();
        throw new IllegalArgumentException("WF-430: Span has too many annotations (" + annotations.size() + ", max " +
            config.getSpanAnnotationsCountLimit() + ")");
      }
      for (Annotation annotation : annotations) {
        final String tagK = annotation.getKey();
        final String tagV = annotation.getValue();
        // Each tag of the form "k=v" must be < 256
        if (tagK.length() + tagV.length() >= 255) {
          ERROR_COUNTERS.get("spanAnnotationTooLong").inc();
          throw new IllegalArgumentException("WF-431: Span annotation (key+value) too long (" +
              (tagK.length() + tagV.length() + 1) + " characters, max: 256): " + tagK + "=" + tagV);
        }
        if (tagK.length() > config.getSpanAnnotationsKeyLengthLimit()) {
          ERROR_COUNTERS.get("spanAnnotationKeyTooLong").inc();
          throw new IllegalArgumentException("WF-432: Span annotation key is too long (" + tagK.length() +
              " characters, max: " + config.getSpanAnnotationsKeyLengthLimit() + "): " + tagK);
        }
        if (!charactersAreValid(tagK)) {
          ERROR_COUNTERS.get("spanAnnotationKeyBadChars").inc();
          throw new IllegalArgumentException("WF-416: Point tag key has illegal character(s): " + tagK);
        }
        if (tagV.length() > config.getSpanAnnotationsValueLengthLimit()) {
          if (blockedLoggingRateLimiter.tryAcquire()){
            log.warning("[" + span.getCustomer() + "] Span trimmed due to tagV length: " +
                annotation.getKey() + " limit for: " + span.getCustomer() + " is: " +
                config.getAnnotationsValueLengthLimit() + ", found: " + annotation.getValue().length()
                + ", span: " + span);
            // trim the tag value to the allowed limit
            annotation.setValue(annotation.getValue().substring(0, config.getAnnotationsValueLengthLimit()));
            TRIM_COUNTERS.get("spanAnnotationValueTooLong").inc();
          }
        }
      }
    }
  }

  /**
   * Legacy point validator
   */
  @Deprecated
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
      ERROR_COUNTERS.get("badchars").inc();
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
