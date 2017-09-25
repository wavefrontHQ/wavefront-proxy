package com.wavefront.common;

import com.google.common.collect.ImmutableMap;

import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;
import com.yammer.metrics.core.VirtualMachineMetrics;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;

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
        .put("mean", metered.meanRate())
        .put("m1", metered.oneMinuteRate())
        .put("m5", metered.fiveMinuteRate())
        .put("m15", metered.fifteenMinuteRate())
        .build();
  }

  public static Map<String, Supplier<Double>> memoryMetrics(VirtualMachineMetrics vm) {
    return ImmutableMap.<String, Supplier<Double>>builder()
        .put("totalInit", vm::totalInit)
        .put("totalUsed", vm::totalUsed)
        .put("totalMax", vm::totalMax)
        .put("totalCommitted", vm::totalCommitted)
        .put("heapInit", vm::heapInit)
        .put("heapUsed", vm::heapUsed)
        .put("heapMax", vm::heapMax)
        .put("heapCommitted", vm::heapCommitted)
        .put("heap_usage", vm::heapUsage)
        .put("non_heap_usage", vm::nonHeapUsage)
        .build();
  }

  public static Map<String, Supplier<Double>> memoryPoolsMetrics(VirtualMachineMetrics vm) {
    ImmutableMap.Builder<String, Supplier<Double>> builder = ImmutableMap.builder();
    for (Map.Entry<String, Double> pool : vm.memoryPoolUsage().entrySet()) {
      builder.put(pool.getKey(), pool::getValue);
    }
    return builder.build();
  }

  public static Map<String, Supplier<Double>> buffersMetrics(VirtualMachineMetrics.BufferPoolStats bps) {
    return ImmutableMap.<String, Supplier<Double>>builder()
        .put("count", () -> (double) bps.getCount())
        .put("memoryUsed", () -> (double) bps.getMemoryUsed())
        .put("totalCapacity", () -> (double) bps.getTotalCapacity())
        .build();
  }

  public static Map<String, Supplier<Double>> vmMetrics(VirtualMachineMetrics vm) {
    return ImmutableMap.<String, Supplier<Double>>builder()
        .put("daemon_thread_count", () -> (double) vm.daemonThreadCount())
        .put("thread_count", () -> (double) vm.threadCount())
        .put("uptime", () -> (double) vm.uptime())
        .put("fd_usage", vm::fileDescriptorUsage)
        .build();
  }

  public static Map<String, Supplier<Double>> threadStateMetrics(VirtualMachineMetrics vm) {
    ImmutableMap.Builder<String, Supplier<Double>> builder = ImmutableMap.builder();
    for (Map.Entry<Thread.State, Double> entry : vm.threadStatePercentages().entrySet()) {
      builder.put(entry.getKey().toString().toLowerCase(), entry::getValue);
    }
    return builder.build();
  }

  public static Map<String, Supplier<Double>> gcMetrics(VirtualMachineMetrics.GarbageCollectorStats gcs) {
    return ImmutableMap.<String, Supplier<Double>>builder()
        .put("runs", () -> (double) gcs.getRuns())
        .put("time", () -> (double) gcs.getTime(TimeUnit.MILLISECONDS))
        .build();
  }


  private static final Pattern SIMPLE_NAMES = Pattern.compile("[^a-zA-Z0-9_.\\-~]");

  public static String sanitize(String name) {
    return SIMPLE_NAMES.matcher(name).replaceAll("_");
  }

  public static String sanitize(MetricName metricName) {
    return sanitize(metricName.getGroup() + "." + metricName.getName());
  }

}
