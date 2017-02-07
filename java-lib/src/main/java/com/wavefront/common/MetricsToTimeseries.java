package com.wavefront.common;

import com.google.common.collect.ImmutableMap;

import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;

import java.util.Map;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public abstract class MetricsToTimeseries {

  public static Map<String, Double> explodeSummarizable(Summarizable metric) {
    return ImmutableMap.<String, Double>builder()
        .put("min", metric.min())
        .put("max", metric.max())
        .put("mean", metric.mean())
        .put("sum", metric.sum())
        .put("stddev", metric.stdDev())
        .build();
  }

  public static Map<String, Double> explodeSampling(Sampling sampling) {
    return ImmutableMap.<String, Double>builder()
        .put("median", sampling.getSnapshot().getMedian())
        .put("p75", sampling.getSnapshot().get75thPercentile())
        .put("p95", sampling.getSnapshot().get95thPercentile())
        .put("p99", sampling.getSnapshot().get99thPercentile())
        .put("p999", sampling.getSnapshot().get999thPercentile())
        .build();
  }

  public static Map<String, Double> explodeMetered(Metered metered) {
    return ImmutableMap.<String, Double>builder()
        .put("count", new Long(metered.count()).doubleValue())
        .put("mean", metered.oneMinuteRate())
        .put("m1", metered.oneMinuteRate())
        .put("m5", metered.fiveMinuteRate())
        .put("m15", metered.fifteenMinuteRate())
        .build();
  }
}
