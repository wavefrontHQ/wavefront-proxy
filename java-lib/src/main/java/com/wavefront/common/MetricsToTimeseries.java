package com.wavefront.common;

import com.google.common.collect.ImmutableMap;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.VirtualMachineMetrics;
import com.yammer.metrics.core.WavefrontHistogram;
import com.yammer.metrics.stats.Snapshot;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public abstract class MetricsToTimeseries {

  public static Map<String, Double> explodeSummarizable(Summarizable metric) {
    return explodeSummarizable(metric, false);
  }

  /**
   * Retrieve values for pre-defined stats (min/max/mean/sum/stddev) from a {@link Summarizable} metric (e.g. histogram)
   *
   * @param metric            metric to retrieve stats from
   * @param convertNanToZero  simulate {@link com.yammer.metrics.core.Histogram} histogram behavior when used with
   *                          {@link com.yammer.metrics.core.WavefrontHistogram} as input:
   *                          when "true", empty WavefrontHistogram reports zero values for all stats
   * @return summarizable stats
   */
  public static Map<String, Double> explodeSummarizable(Summarizable metric, boolean convertNanToZero) {
    ImmutableMap.Builder<String, Double> builder = ImmutableMap.<String, Double>builder()
        .put("min", sanitizeValue(metric.min(), convertNanToZero))
        .put("max", sanitizeValue(metric.max(), convertNanToZero))
        .put("mean", sanitizeValue(metric.mean(), convertNanToZero));
    if (metric instanceof Histogram || metric instanceof Timer) {
      builder.put("sum", sanitizeValue(metric.sum(), convertNanToZero));
      builder.put("stddev", sanitizeValue(metric.stdDev(), convertNanToZero));
    }
    return builder.build();
  }

  public static Map<String, Double> explodeSampling(Sampling sampling) {
    return explodeSampling(sampling, false);
  }

  /**
   * Retrieve values for pre-defined stats (median/p75/p95/p99/p999) from a {@link Sampling} metric (e.g. histogram)
   *
   * @param sampling          metric to retrieve stats from
   * @param convertNanToZero  simulate {@link com.yammer.metrics.core.Histogram} histogram behavior when used with
   *                          {@link com.yammer.metrics.core.WavefrontHistogram} as input:
   *                          when "true", empty WavefrontHistogram reports zero values for all stats
   * @return sampling stats
   */
  public static Map<String, Double> explodeSampling(Sampling sampling, boolean convertNanToZero) {
    final Snapshot snapshot = sampling.getSnapshot();
    return ImmutableMap.<String, Double>builder()
        .put("median", sanitizeValue(snapshot.getMedian(), convertNanToZero))
        .put("p75", sanitizeValue(snapshot.get75thPercentile(), convertNanToZero))
        .put("p95", sanitizeValue(snapshot.get95thPercentile(), convertNanToZero))
        .put("p99", sanitizeValue(snapshot.get99thPercentile(), convertNanToZero))
        .put("p999", sanitizeValue(snapshot.get999thPercentile(), convertNanToZero))
        .build();
  }

  public static Map<String, Double> explodeMetered(Metered metered) {
    return ImmutableMap.<String, Double>builder()
        .put("count", new Long(metered.count()).doubleValue())
        .put("mean", metered.meanRate())
        .put("m1", metered.oneMinuteRate())
        .put("m5", metered.fiveMinuteRate())
        .put("m15", metered.fifteenMinuteRate())
        .build();
  }

  public static Map<String, Double> memoryMetrics(VirtualMachineMetrics vm) {
    return ImmutableMap.<String, Double>builder()
        .put("totalInit", vm.totalInit())
        .put("totalUsed", vm.totalUsed())
        .put("totalMax", vm.totalMax())
        .put("totalCommitted", vm.totalCommitted())
        .put("heapInit", vm.heapInit())
        .put("heapUsed", vm.heapUsed())
        .put("heapMax", vm.heapMax())
        .put("heapCommitted", vm.heapCommitted())
        .put("heap_usage", vm.heapUsage())
        .put("non_heap_usage", vm.nonHeapUsage())
        .build();
  }

  public static Map<String, Double> memoryPoolsMetrics(VirtualMachineMetrics vm) {
    ImmutableMap.Builder<String, Double> builder = ImmutableMap.builder();
    for (Map.Entry<String, Double> pool : vm.memoryPoolUsage().entrySet()) {
      builder.put(pool.getKey(), pool.getValue());
    }
    return builder.build();
  }

  public static Map<String, Double> buffersMetrics(VirtualMachineMetrics.BufferPoolStats bps) {
    return ImmutableMap.<String, Double>builder()
        .put("count", (double) bps.getCount())
        .put("memoryUsed", (double) bps.getMemoryUsed())
        .put("totalCapacity", (double) bps.getTotalCapacity())
        .build();
  }

  public static Map<String, Double> vmMetrics(VirtualMachineMetrics vm) {
    return ImmutableMap.<String, Double>builder()
        .put("daemon_thread_count", (double) vm.daemonThreadCount())
        .put("thread_count", (double) vm.threadCount())
        .put("uptime", (double) vm.uptime())
        .put("fd_usage", vm.fileDescriptorUsage())
        .build();
  }

  public static Map<String, Double> threadStateMetrics(VirtualMachineMetrics vm) {
    ImmutableMap.Builder<String, Double> builder = ImmutableMap.builder();
    for (Map.Entry<Thread.State, Double> entry : vm.threadStatePercentages().entrySet()) {
      builder.put(entry.getKey().toString().toLowerCase(), entry.getValue());
    }
    return builder.build();
  }

  public static Map<String, Double> gcMetrics(VirtualMachineMetrics.GarbageCollectorStats gcs) {
    return ImmutableMap.<String, Double>builder()
        .put("runs", (double) gcs.getRuns())
        .put("time", (double) gcs.getTime(TimeUnit.MILLISECONDS))
        .build();
  }


  private static final Pattern SIMPLE_NAMES = Pattern.compile("[^a-zA-Z0-9_.\\-~]");

  public static String sanitize(String name) {
    return SIMPLE_NAMES.matcher(name).replaceAll("_");
  }

  public static String sanitize(MetricName metricName) {
    return sanitize(metricName.getGroup() + "." + metricName.getName());
  }

  public static double sanitizeValue(double value, boolean convertNanToZero) {
    return Double.isNaN(value) && convertNanToZero ? 0 : value;
  }

}
