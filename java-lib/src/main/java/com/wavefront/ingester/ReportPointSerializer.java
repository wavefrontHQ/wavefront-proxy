package com.wavefront.ingester;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;

import java.util.Map;
import java.util.function.Function;

import javax.annotation.Nullable;

import wavefront.report.ReportPoint;

/**
 * Convert a {@link ReportPoint} to its string representation in a canonical format (quoted metric name,
 * tag values and keys (except for "source"). Supports numeric and {@link wavefront.report.Histogram} values.
 *
 * @author vasily@wavefront.com
 */
public class ReportPointSerializer implements Function<ReportPoint, String> {

  @Override
  public String apply(ReportPoint point) {
    return pointToString(point);
  }

  private static String quote = "\"";

  private static String escapeQuotes(String raw) {
    return StringUtils.replace(raw, quote, "\\\"");
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

  @VisibleForTesting
  protected static String pointToString(ReportPoint point) {
    if (point.getValue() instanceof Double || point.getValue() instanceof Long ||
        point.getValue() instanceof String) {
      long originalTimestamp = point.getTimestamp();
      int timestampSize = Long.toString(originalTimestamp).length();
      long timestampSeconds;
      if (timestampSize == 10) {
        timestampSeconds = originalTimestamp;
      } else if (timestampSize == 13) {
        timestampSeconds = originalTimestamp / 1000;
      } else {
        // should never get here ...
        throw new IllegalArgumentException("Invalid timestamp for point: " + point);
      }
      StringBuilder sb = new StringBuilder(quote)
          .append(escapeQuotes(point.getMetric())).append(quote).append(" ")
          .append(point.getValue()).append(" ")
          .append(timestampSeconds).append(" ")
          .append("source=").append(quote).append(escapeQuotes(point.getHost())).append(quote);
      appendTagMap(sb, point.getAnnotations());
      return sb.toString();
    } else if (point.getValue() instanceof wavefront.report.Histogram) {
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
      for (int i = 0; i < numCentroids; ++i) {
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
}
