package com.wavefront.common;

import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Helper functions that convert metric data into Wavefront Data Format syntax
 *
 * @author Han Zhang (zhanghan@vmware.com)
 */
public class WavefrontDataFormat {
  private static final Pattern WHITESPACE = Pattern.compile("[\\s]+");

  public static String pointToString(String name,
                                     double value,
                                     @Nullable Long timestamp,
                                     String source,
                                     @Nullable Map<String, String> pointTags,
                                     boolean appendNewLine) throws IllegalArgumentException {

    if (StringUtils.isBlank(name)) {
      throw new IllegalArgumentException("metric name cannot be blank");
    }
    if (StringUtils.isBlank(source)) {
      throw new IllegalArgumentException("source cannot be blank");
    }
    final StringBuilder sb = new StringBuilder();
    sb.append(sanitize(name));
    sb.append(" ").append(Double.toString(value));
    if (timestamp != null) {
      sb.append(" ").append(Long.toString(timestamp));
    }
    sb.append(" source=").append(sanitize(source));
    sb.append(pointTagsToString(pointTags));
    if (appendNewLine) {
      sb.append("\n");
    }
    return sb.toString();
  }

  public static List<String> histogramToStrings(Set<HistogramGranularity> histogramGranularities,
                                                @Nullable Long timestamp,
                                                List<Pair<Double, Integer>> centroids,
                                                String name,
                                                String source,
                                                @Nullable Map<String, String> pointTags,
                                                boolean appendNewLine) throws IllegalArgumentException {

    if (histogramGranularities == null || histogramGranularities.isEmpty()) {
      throw new IllegalArgumentException("histogram granularities cannot be null or empty");
    }
    if (centroids == null || centroids.isEmpty()) {
      throw new IllegalArgumentException("centroids cannot be null or empty");
    }
    if (StringUtils.isBlank(name)) {
      throw new IllegalArgumentException("distribution name cannot be blank");
    }
    if (StringUtils.isBlank(source)) {
      throw new IllegalArgumentException("source cannot be blank");
    }
    final StringBuilder sb = new StringBuilder();
    if (timestamp != null) {
      sb.append(" ").append(Long.toString(timestamp));
    }
    for (Pair<Double, Integer> centroid : centroids) {
      if (centroid == null) {
        throw new IllegalArgumentException("centroid cannot be null");
      }
      if (centroid._1 == null) {
        throw new IllegalArgumentException("centroid value cannot be null");
      }
      if (centroid._2 == null) {
        throw new IllegalArgumentException("centroid count cannot be null");
      }
      if (centroid._2 <= 0) {
        throw new IllegalArgumentException("centroid count cannot be less than 1");
      }
      sb.append(" #").append(Integer.toString(centroid._2)).append(" ").append(Double.toString(centroid._1));
    }
    sb.append(" ").append(sanitize(name));
    sb.append(" source=").append(sanitize(source));
    sb.append(pointTagsToString(pointTags));
    if (appendNewLine) {
      sb.append("\n");
    }

    List<String> histogramStrings = new ArrayList<>();
    for (HistogramGranularity histogramGranularity : histogramGranularities) {
      if (histogramGranularity == null) {
        throw new IllegalArgumentException("histogram granularity cannot be null");
      }
      histogramStrings.add(histogramGranularity.identifier + sb.toString());
    }
    return histogramStrings;
  }

  static String pointTagsToString(Map<String, String> pointTags) throws IllegalArgumentException {
    StringBuilder sb = new StringBuilder();
    if (pointTags != null) {
      for (final Map.Entry<String, String> tag : pointTags.entrySet()) {
        if (StringUtils.isBlank(tag.getKey())) {
          throw new IllegalArgumentException("point tag key cannot be blank");
        }
        if (StringUtils.isBlank(tag.getValue())) {
          throw new IllegalArgumentException("point tag value cannot be blank");
        }
        sb.append(" ").append(sanitize(tag.getKey())).append("=").append(sanitize(tag.getValue()));
      }
    }
    return sb.toString();
  }

  static String sanitize(String s) {
    final String whitespaceSanitized = WHITESPACE.matcher(s).replaceAll("-");
    if (s.contains("\"") || s.contains("'")) {
      // for single quotes, once we are double-quoted, single quotes can exist happily inside it.
      return "\"" + whitespaceSanitized.replaceAll("\"", "\\\\\"") + "\"";
    } else {
      return "\"" + whitespaceSanitized + "\"";
    }
  }
}
