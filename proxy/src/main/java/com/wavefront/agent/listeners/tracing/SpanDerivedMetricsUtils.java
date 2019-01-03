package com.wavefront.agent.listeners.tracing;

import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.WavefrontHistogram;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import wavefront.report.ReportPoint;

import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.CLUSTER_TAG_KEY;
import static com.wavefront.sdk.common.Constants.COMPONENT_TAG_KEY;
import static com.wavefront.sdk.common.Constants.HEART_BEAT_METRIC;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SHARD_TAG_KEY;

/**
 * Util methods to generate data (metrics/histograms/heartbeats) from tracing spans
 *
 * @author Sushant Dewan (sushant@wavefront.com).
 */
public class SpanDerivedMetricsUtils {

  private final static String TRACING_DERIVED_PREFIX = "tracing.derived";
  private final static String INVOCATION_SUFFIX = ".invocation";
  private final static String ERROR_SUFFIX = ".error";
  private final static String DURATION_SUFFIX = ".duration.micros";
  private final static String TOTAL_TIME_SUFFIX = "total_time.millis";
  private final static String OPERATION_NAME_TAG = "operationName";
  public final static String ERROR_SPAN_TAG_KEY = "error";
  public final static String ERROR_SPAN_TAG_VAL = "true";

  /**
   * Report generated metrics and histograms from the wavefront tracing span.
   *
   * @param operationName          span operation name
   * @param application            name of the application
   * @param service                name of the service
   * @param cluster                name of the cluster
   * @param shard                  name of the shard
   * @param source                 reporting source
   * @param isError                indicates if the span is erroneous
   * @param spanDurationMicros     Original span duration (both Zipkin and Jaeger support micros
   *                               duration).
   * @return HeartbeatMetricKey so that it is added to discovered keys.
   */
  static HeartbeatMetricKey reportWavefrontGeneratedData(String operationName, String application,
                                                         String service, String cluster,
                                                         String shard, String source,
                                                         boolean isError, long spanDurationMicros) {
    /*
     * 1) Can only propagate mandatory application/service and optional cluster/shard tags.
     * 2) Cannot convert ApplicationTags.customTags unfortunately as those are not well-known.
     * 3) Both Jaeger and Zipkin support error=true tag for erroneous spans
     */
    // tracing.derived.<application>.<service>.<operation>.invocation.count
    Metrics.newCounter(new TaggedMetricName(TRACING_DERIVED_PREFIX,
        sanitize(application + "." + service + "." + operationName + INVOCATION_SUFFIX),
        APPLICATION_TAG_KEY, application, SERVICE_TAG_KEY, service, CLUSTER_TAG_KEY,
        cluster, SHARD_TAG_KEY, shard, OPERATION_NAME_TAG, operationName)).inc();
    if (isError) {
      // tracing.derived.<application>.<service>.<operation>.error.count
      Metrics.newCounter(new TaggedMetricName(TRACING_DERIVED_PREFIX,
          sanitize(application + "." + service + "." + operationName + ERROR_SUFFIX),
          APPLICATION_TAG_KEY, application, SERVICE_TAG_KEY, service, CLUSTER_TAG_KEY,
          cluster, SHARD_TAG_KEY, shard, OPERATION_NAME_TAG, operationName)).inc();
    }

    // tracing.derived.<application>.<service>.<operation>.duration.micros.m
    WavefrontHistogram.get(new TaggedMetricName(TRACING_DERIVED_PREFIX,
        sanitize(application + "." + service + "." + operationName + DURATION_SUFFIX),
        APPLICATION_TAG_KEY, application, SERVICE_TAG_KEY, service, CLUSTER_TAG_KEY,
        cluster, SHARD_TAG_KEY, shard, OPERATION_NAME_TAG, operationName)).
        update(spanDurationMicros);

    // tracing.derived.<application>.<service>.<operation>.total_time.millis.count
    Metrics.newCounter(new TaggedMetricName(TRACING_DERIVED_PREFIX,
        sanitize(application + "." + service + "." + operationName + TOTAL_TIME_SUFFIX),
        APPLICATION_TAG_KEY, application, SERVICE_TAG_KEY, service, CLUSTER_TAG_KEY,
        cluster, SHARD_TAG_KEY, shard, OPERATION_NAME_TAG, operationName))
        .inc(spanDurationMicros / 10000);
    return new HeartbeatMetricKey(application, service, cluster, shard, source);
  }

  private static String sanitize(String s) {
    Pattern WHITESPACE = Pattern.compile("[\\s]+");
    final String whitespaceSanitized = WHITESPACE.matcher(s).replaceAll("-");
    if (s.contains("\"") || s.contains("'")) {
      // for single quotes, once we are double-quoted, single quotes can exist happily inside it.
      return whitespaceSanitized.replaceAll("\"", "\\\\\"");
    } else {
      return whitespaceSanitized;
    }
  }

  /**
   * Report discovered heartbeats to Wavefront.
   *
   * @param pointHandler                Handler to report metric points to Wavefront.
   * @param discoveredHeartbeatMetrics  Discovered heartbeats.
   */
  static void reportHeartbeats(String component,
      ReportableEntityHandler<ReportPoint> pointHandler,
      Map<HeartbeatMetricKey, Boolean> discoveredHeartbeatMetrics) {
    if (pointHandler == null) {
      // should never happen
      return;
    }
    Iterator<HeartbeatMetricKey> iter = discoveredHeartbeatMetrics.keySet().iterator();
    while (iter.hasNext()) {
      HeartbeatMetricKey key = iter.next();
      ReportPoint heartbeatPoint = ReportPoint.newBuilder().
          setTable("dummy").
          setMetric(HEART_BEAT_METRIC).
          setHost(key.getSource()).
          setTimestamp(System.currentTimeMillis()).
          setAnnotations(new HashMap<String, String>() {{
            put(APPLICATION_TAG_KEY, key.getApplication());
            put(SERVICE_TAG_KEY, key.getService());
            put(CLUSTER_TAG_KEY, key.getCluster());
            put(SHARD_TAG_KEY, key.getShard());
            put(COMPONENT_TAG_KEY, component);
          }}).setValue(1L).build();
      pointHandler.report(heartbeatPoint);
      // remove from discovered list so that it is only reported on subsequent discovery
      discoveredHeartbeatMetrics.remove(key);
    }
  }
}
