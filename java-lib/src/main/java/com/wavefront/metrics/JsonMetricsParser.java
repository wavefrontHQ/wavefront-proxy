package com.wavefront.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import sunnylabs.report.Histogram;
import sunnylabs.report.HistogramType;
import sunnylabs.report.ReportPoint;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Helper methods to turn json nodes into actual Wavefront report points
 *
 * @author Andrew Kao (andrew@wavefront.com)
 */
public class JsonMetricsParser {
  private static final Pattern SIMPLE_NAMES = Pattern.compile("[^a-zA-Z0-9_.-]");
  private static final Pattern TAGGED_METRIC = Pattern.compile("(.*)\\$[0-9-]+");

  public static void report(String table, String path, JsonNode node, List<ReportPoint> points, String host,
                            long timestamp) {
    report(table, path, node, points, host, timestamp, Collections.<String, String>emptyMap());
  }

  public static void report(String table, String path, JsonNode node, List<ReportPoint> points, String host,
                            long timestamp, Map<String, String> tags) {
    List<Map.Entry<String, JsonNode>> fields = newArrayList(node.fields());
    // if the node only has the follow nodes: "value" and "tags", parse the node as a value with tags.
    if (fields.size() == 2) {
      JsonNode valueNode = null;
      JsonNode tagsNode = null;
      for (Map.Entry<String, JsonNode> next : fields) {
        if (next.getKey().equals("value")) {
          valueNode = next.getValue();
        } else if (next.getKey().equals("tags")) {
          tagsNode = next.getValue();
        }
      }
      if (valueNode != null && tagsNode != null) {
        Map<String, String> combinedTags = Maps.newHashMap(tags);
        combinedTags.putAll(makeTags(tagsNode));
        processValueNode(valueNode, table, path, host, timestamp, points, combinedTags);
        return;
      }
    }
    for (Map.Entry<String, JsonNode> next : fields) {
      String key;
      Matcher taggedMetricMatcher = TAGGED_METRIC.matcher(next.getKey());
      if (taggedMetricMatcher.matches()) {
        key = SIMPLE_NAMES.matcher(taggedMetricMatcher.group(1)).replaceAll("_");
      } else {
        key = SIMPLE_NAMES.matcher(next.getKey()).replaceAll("_");
      }
      String metric = path == null ? key : path + "." + key;
      JsonNode value = next.getValue();
      processValueNode(value, table, metric, host, timestamp, points, tags);
    }
  }

  public static void processValueNode(JsonNode value, String table, String metric, String host, long timestamp,
                                      List<ReportPoint> points, Map<String, String> tags) {
    if (value.isNumber()) {
      if (value.isLong()) {
        points.add(makePoint(table, metric, host, value.longValue(), timestamp, tags));
      } else {
        points.add(makePoint(table, metric, host, value.doubleValue(), timestamp, tags));
      }
    } else if (value.isTextual()) {
      points.add(makePoint(table, metric, host, value.textValue(), timestamp, tags));
    } else if (value.isObject()) {
      if /*wavefront histogram*/ (value.has("bins")) {
        Iterator<JsonNode> binIt = ((ArrayNode) value.get("bins")).elements();
        while (binIt.hasNext()) {
          JsonNode bin = binIt.next();
          List<Integer> counts = newArrayList();
          bin.get("counts").elements().forEachRemaining(v -> counts.add(v.intValue()));
          List<Double> means = newArrayList();
          bin.get("means").elements().forEachRemaining(v -> means.add(v.doubleValue()));

          points.add(makeHistogramPoint(
              table,
              metric,
              host,
              tags,
              bin.get("startMillis").longValue(),
              bin.get("durationMillis").intValue(),
              means,
              counts));
        }


      } else {
        report(table, metric, value, points, host, timestamp, tags);
      }
    } else if (value.isBoolean()) {
      points.add(makePoint(table, metric, host, value.booleanValue() ? 1.0 : 0.0, timestamp, tags));
    }
  }

  public static ReportPoint makeHistogramPoint(
      String customer,
      String metric,
      String host,
      Map<String, String> annotations,
      long startMillis,
      int durationMillis,
      List<Double> bins,
      List<Integer> counts) {
    Histogram histogram = Histogram.newBuilder()
        .setType(HistogramType.TDIGEST)
        .setDuration(durationMillis)
        .setCounts(counts)
        .setBins(bins).build();

    return makePoint(customer, metric, host, startMillis, annotations).setValue(histogram).build();
  }

  public static ReportPoint makePoint(String table, String metric, String host, String value, long timestamp) {
    return makePoint(table, metric, host, value, timestamp, Collections.<String, String>emptyMap());
  }

  public static ReportPoint makePoint(String table, String metric, String host, String value, long timestamp,
                                      Map<String, String> annotations) {
    ReportPoint.Builder builder = makePoint(table, metric, host, timestamp, annotations);
    return builder.setValue(value).build();
  }

  public static ReportPoint makePoint(String table, String metric, String host, long value, long timestamp) {
    return makePoint(table, metric, host, value, timestamp, Collections.<String, String>emptyMap());
  }

  public static ReportPoint makePoint(String table, String metric, String host, long value, long timestamp,
                                      Map<String, String> annotations) {
    ReportPoint.Builder builder = makePoint(table, metric, host, timestamp, annotations);
    return builder.setValue(value).build();
  }

  public static ReportPoint makePoint(String table, String metric, String host, double value, long timestamp) {
    return makePoint(table, metric, host, value, timestamp, Collections.<String, String>emptyMap());
  }

  public static ReportPoint makePoint(String table, String metric, String host, double value, long timestamp,
                                      Map<String, String> annotations) {
    ReportPoint.Builder builder = makePoint(table, metric, host, timestamp, annotations);
    return builder.setValue(value).build();
  }

  private static ReportPoint.Builder makePoint(String table, String metric, String host, long timestamp,
                                               Map<String, String> annotations) {
    return ReportPoint.newBuilder()
        .setAnnotations(annotations)
        .setMetric(metric)
        .setTable(table)
        .setTimestamp(timestamp)
        .setHost(host);
  }

  public static Map<String, String> makeTags(JsonNode tags) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    if (tags.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> fields = tags.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> next = fields.next();
        String key = SIMPLE_NAMES.matcher(next.getKey()).replaceAll("_");
        JsonNode value = next.getValue();
        if (value.isBoolean()) {
          builder.put(key, String.valueOf(value.booleanValue()));
        } else if (value.isNumber()) {
          if (value.isLong()) {
            builder.put(key, String.valueOf(value.asLong()));
          } else {
            builder.put(key, String.valueOf(value.asDouble()));
          }
        } else if (value.isTextual()) {
          builder.put(key, value.asText());
        }
      }
    }
    return builder.build();
  }
}
