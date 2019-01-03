package com.wavefront.agent.listeners.tracing;

import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.WavefrontHistogram;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import wavefront.report.Annotation;
import wavefront.report.ReportPoint;
import wavefront.report.Span;

import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.CLUSTER_TAG_KEY;
import static com.wavefront.sdk.common.Constants.COMPONENT_TAG_KEY;
import static com.wavefront.sdk.common.Constants.HEART_BEAT_METRIC;
import static com.wavefront.sdk.common.Constants.NULL_TAG_VAL;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SHARD_TAG_KEY;

/**
 * // TODO - javadoc
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

  /**
   * Report generated metrics and histograms from the wavefront tracing span.
   *
   * @param span               Wavefront tracing span.
   * @param spanDurationMicros Original span duration (both Zipkin and Jaeger support micros
   *                           duration).
   * @return HeartbeatMetricKey so that it is added to discovered keys
   */
  static HeartbeatMetricKey reportWavefrontGeneratedData(Span span, long spanDurationMicros) {
    /*
     * 1) Can only propagate mandatory application/service and optional cluster/shard tags.
     * 2) Cannot convert ApplicationTags.customTags unfortunately as those are not well-known.
     * 3) Both Jaeger and Zipkin support error=true tag for erroneous spans
     */
    String applicationName = NULL_TAG_VAL;
    String serviceName = NULL_TAG_VAL;
    String cluster = NULL_TAG_VAL;
    String shard = NULL_TAG_VAL;
    boolean isError = false;

    for (Annotation annotation : span.getAnnotations()) {
      switch (annotation.getKey()) {
        case APPLICATION_TAG_KEY:
          applicationName = annotation.getValue();
          continue;
        case SERVICE_TAG_KEY:
          serviceName = annotation.getValue();
          continue;
        case CLUSTER_TAG_KEY:
          cluster = annotation.getValue();
          continue;
        case SHARD_TAG_KEY:
          shard = annotation.getValue();
          continue;
        case "error":
          // only error=true is supported
          isError = annotation.getValue().equals("true");
      }
    }

    // tracing.derived.<application>.<service>.<operation>.invocation.count
    Metrics.newCounter(new TaggedMetricName(TRACING_DERIVED_PREFIX,
        applicationName + "." + serviceName + "." + span.getName() + INVOCATION_SUFFIX,
        APPLICATION_TAG_KEY, applicationName, SERVICE_TAG_KEY, serviceName, CLUSTER_TAG_KEY,
        cluster, SHARD_TAG_KEY, shard, OPERATION_NAME_TAG, span.getName())).inc();
    if (isError) {
      // tracing.derived.<application>.<service>.<operation>.error.count
      Metrics.newCounter(new TaggedMetricName(TRACING_DERIVED_PREFIX,
          applicationName + "." + serviceName + "." + span.getName() + ERROR_SUFFIX,
          APPLICATION_TAG_KEY, applicationName, SERVICE_TAG_KEY, serviceName, CLUSTER_TAG_KEY,
          cluster, SHARD_TAG_KEY, shard, OPERATION_NAME_TAG, span.getName())).inc();
    }

    // tracing.derived.<application>.<service>.<operation>.duration.micros.m
    WavefrontHistogram.get(new TaggedMetricName(TRACING_DERIVED_PREFIX,
        applicationName + "." + serviceName + "." + span.getName() + DURATION_SUFFIX,
        APPLICATION_TAG_KEY, applicationName, SERVICE_TAG_KEY, serviceName, CLUSTER_TAG_KEY,
        cluster, SHARD_TAG_KEY, shard, OPERATION_NAME_TAG, span.getName())).
        update(spanDurationMicros);

    // tracing.derived.<application>.<service>.<operation>.error.count
    Metrics.newCounter(new TaggedMetricName(TRACING_DERIVED_PREFIX,
        applicationName + "." + serviceName + "." + span.getName() + TOTAL_TIME_SUFFIX,
        APPLICATION_TAG_KEY, applicationName, SERVICE_TAG_KEY, serviceName, CLUSTER_TAG_KEY,
        cluster, SHARD_TAG_KEY, shard, OPERATION_NAME_TAG, span.getName())).inc(span.getDuration());
    return new HeartbeatMetricKey(applicationName, serviceName, cluster, shard, span.getSource());
  }

  /**
   * Report discovered heartbeats to Wavefront
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
      iter.remove();
    }
  }
}
