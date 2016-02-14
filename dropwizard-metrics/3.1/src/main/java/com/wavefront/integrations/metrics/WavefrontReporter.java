package com.wavefront.integrations.metrics;

import com.codahale.metrics.*;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.wavefront.integrations.Wavefront;
import com.wavefront.integrations.WavefrontSender;

import java.lang.management.ManagementFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * A reporter which publishes metric values to a Wavefront Proxy.
 *
 */
public class WavefrontReporter extends ScheduledReporter {
  /**
   * Returns a new {@link Builder} for {@link WavefrontReporter}.
   *
   * @param registry the registry to report
   * @return a {@link Builder} instance for a {@link WavefrontReporter}
   */
  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  /**
   * A builder for {@link WavefrontReporter} instances. Defaults to not using a prefix, using the
   * default clock, converting rates to events/second, converting durations to milliseconds,
   * a host named "unknown", no point Tags, and not filtering any metrics.
   */
  public static class Builder {
    private final MetricRegistry registry;
    private Clock clock;
    private String prefix;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;
    private String source;
    private Map<String, String> pointTags;
    private boolean includeJvmMetrics;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.clock = Clock.defaultClock();
      this.prefix = null;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
      this.source = "dropwizard-metrics";
      this.pointTags = new HashMap<String, String>();
      this.includeJvmMetrics = false;
    }

    /**
     * Use the given {@link Clock} instance for the time. Defaults to Clock.defaultClock()
     *
     * @param clock a {@link Clock} instance
     * @return {@code this}
     */
    public Builder withClock(Clock clock) {
      this.clock = clock;
      return this;
    }

    /**
     * Prefix all metric names with the given string. Defaults to null.
     *
     * @param prefix the prefix for all metric names
     * @return {@code this}
     */
    public Builder prefixedWith(String prefix) {
      this.prefix = prefix;
      return this;
    }

    /**
     * Set the host for this reporter. This is equivalent to withSource.
     *
     * @param host the host for all metrics
     * @return {@code this}
     */
    public Builder withHost(String host) {
      this.source = host;
      return this;
    }

    /**
     * Set the source for this reporter. This is equivalent to withHost.
     *
     * @param source the host for all metrics
     * @return {@code this}
     */
    public Builder withSource(String source) {
      this.source = source;
      return this;
    }

    /**
     * Set the Point Tags for this reporter.
     *
     * @param pointTags the pointTags Map for all metrics
     * @return {@code this}
     */
    public Builder withPointTags(Map<String, String> pointTags) {
      this.pointTags.putAll(pointTags);
      return this;
    }

    /**
     * Set a point tag for this reporter.
     *
     * @param ptagK the key of the Point Tag
     * @param ptagV the value of the Point Tag
     * @return {@code this}
     */
    public Builder withPointTag(String ptagK, String ptagV) {
      this.pointTags.put(ptagK, ptagV);
      return this;
    }

    /**
     * Convert rates to the given time unit. Defaults to Seconds.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    /**
     * Convert durations to the given time unit. Defaults to Milliseconds.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    /**
     * Only report metrics which match the given filter. Defaults to MetricFilter.ALL
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    /**
     * Include JVM Metrics from this Reporter.
     *
     * @return
     */
    public Builder withJvmMetrics() {
      this.includeJvmMetrics = true;
      return this;
    }

    /**
     * Builds a {@link WavefrontReporter} with the given properties, sending metrics using the
     * given {@link WavefrontSender}.
     *
     * @param Wavefront a {@link WavefrontSender}
     * @return a {@link WavefrontReporter}
     */
    public WavefrontReporter build(String proxyHostname, int proxyPort) {
      return new WavefrontReporter(registry,
                                   proxyHostname,
                                   proxyPort,
                                   clock,
                                   prefix,
                                   source,
                                   pointTags,
                                   rateUnit,
                                   durationUnit,
                                   filter,
                                   includeJvmMetrics);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(WavefrontReporter.class);

  private final WavefrontSender wavefront;
  private final Clock clock;
  private final String prefix;
  private final String source;
  private final Map<String, String> pointTags;

  private WavefrontReporter(MetricRegistry registry,
                            String proxyHostname,
                            int proxyPort,
                            final Clock clock,
                            String prefix,
                            String source,
                            Map<String, String> pointTags,
                            TimeUnit rateUnit,
                            TimeUnit durationUnit,
                            MetricFilter filter,
                            boolean includeJvmMetrics) {
    super(registry, "wavefront-reporter", filter, rateUnit, durationUnit);
    this.wavefront = new Wavefront(proxyHostname, proxyPort);
    this.clock = clock;
    this.prefix = prefix;
    this.source = source;
    this.pointTags = pointTags;

    if (includeJvmMetrics) {
      registry.register("jvm.uptime", new Gauge<Long>() {
          @Override
          public Long getValue() {
            return ManagementFactory.getRuntimeMXBean().getUptime();
          }
        });
      registry.register("jvm.current_time", new Gauge<Long>() {
          @Override
          public Long getValue() {
            return clock.getTime();
          }
        });
      registry.register("jvm.classes", new ClassLoadingGaugeSet());
      registry.register("jvm.fd_usage", new FileDescriptorRatioGauge());
      registry.register("jvm.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
      registry.register("jvm.gc", new GarbageCollectorMetricSet());
      registry.register("jvm.memory", new MemoryUsageGaugeSet());
    }
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges,
                     SortedMap<String, Counter> counters,
                     SortedMap<String, Histogram> histograms,
                     SortedMap<String, Meter> meters,
                     SortedMap<String, Timer> timers) {
    final long timestamp = clock.getTime() / 1000;

    try {
      if (!wavefront.isConnected()) {
        wavefront.connect();
      }

      for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
        if (entry.getValue() instanceof Number) {
          reportGauge(entry.getKey(), entry.getValue(), timestamp);
        }
      }

      for (Map.Entry<String, Counter> entry : counters.entrySet()) {
        reportCounter(entry.getKey(), entry.getValue(), timestamp);
      }

      for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
        reportHistogram(entry.getKey(), entry.getValue(), timestamp);
      }

      for (Map.Entry<String, Meter> entry : meters.entrySet()) {
        reportMetered(entry.getKey(), entry.getValue(), timestamp);
      }

      for (Map.Entry<String, Timer> entry : timers.entrySet()) {
        reportTimer(entry.getKey(), entry.getValue(), timestamp);
      }

      wavefront.flush();
    } catch (IOException e) {
      LOGGER.warn("Unable to report to Wavefront", wavefront, e);
      try {
        wavefront.close();
      } catch (IOException e1) {
        LOGGER.warn("Error closing Wavefront", wavefront, e);
      }
    }
  }

  @Override
  public void stop() {
    try {
      super.stop();
    } finally {
      try {
        wavefront.close();
      } catch (IOException e) {
        LOGGER.debug("Error disconnecting from Wavefront", wavefront, e);
      }
    }
  }

  private void reportTimer(String name, Timer timer, long timestamp) throws IOException {
    final Snapshot snapshot = timer.getSnapshot();

    wavefront.send(prefix(name, "max"), convertDuration(snapshot.getMax()), timestamp, source, pointTags);
    wavefront.send(prefix(name, "mean"), convertDuration(snapshot.getMean()), timestamp, source, pointTags);
    wavefront.send(prefix(name, "min"), convertDuration(snapshot.getMin()), timestamp, source, pointTags);
    wavefront.send(prefix(name, "stddev"),
                   convertDuration(snapshot.getStdDev()),
                   timestamp, source, pointTags);
    wavefront.send(prefix(name, "p50"),
                   convertDuration(snapshot.getMedian()),
                   timestamp, source, pointTags);
    wavefront.send(prefix(name, "p75"),
                   convertDuration(snapshot.get75thPercentile()),
                   timestamp, source, pointTags);
    wavefront.send(prefix(name, "p95"),
                   convertDuration(snapshot.get95thPercentile()),
                   timestamp, source, pointTags);
    wavefront.send(prefix(name, "p98"),
                   convertDuration(snapshot.get98thPercentile()),
                   timestamp, source, pointTags);
    wavefront.send(prefix(name, "p99"),
                   convertDuration(snapshot.get99thPercentile()),
                   timestamp, source, pointTags);
    wavefront.send(prefix(name, "p999"),
                   convertDuration(snapshot.get999thPercentile()),
                   timestamp, source, pointTags);

    reportMetered(name, timer, timestamp);
  }

  private void reportMetered(String name, Metered meter, long timestamp) throws IOException {
    wavefront.send(prefix(name, "count"), meter.getCount(), timestamp, source, pointTags);
    wavefront.send(prefix(name, "m1_rate"),
                   convertRate(meter.getOneMinuteRate()),
                   timestamp, source, pointTags);
    wavefront.send(prefix(name, "m5_rate"),
                   convertRate(meter.getFiveMinuteRate()),
                   timestamp, source, pointTags);
    wavefront.send(prefix(name, "m15_rate"),
                   convertRate(meter.getFifteenMinuteRate()),
                   timestamp, source, pointTags);
    wavefront.send(prefix(name, "mean_rate"),
                   convertRate(meter.getMeanRate()),
                   timestamp, source, pointTags);
  }

  private void reportHistogram(String name, Histogram histogram, long timestamp) throws IOException {
    final Snapshot snapshot = histogram.getSnapshot();
    wavefront.send(prefix(name, "count"), histogram.getCount(), timestamp, source, pointTags);
    wavefront.send(prefix(name, "max"), snapshot.getMax(), timestamp, source, pointTags);
    wavefront.send(prefix(name, "mean"), snapshot.getMean(), timestamp, source, pointTags);
    wavefront.send(prefix(name, "min"), snapshot.getMin(), timestamp, source, pointTags);
    wavefront.send(prefix(name, "stddev"), snapshot.getStdDev(), timestamp, source, pointTags);
    wavefront.send(prefix(name, "p50"), snapshot.getMedian(), timestamp, source, pointTags);
    wavefront.send(prefix(name, "p75"), snapshot.get75thPercentile(), timestamp, source, pointTags);
    wavefront.send(prefix(name, "p95"), snapshot.get95thPercentile(), timestamp, source, pointTags);
    wavefront.send(prefix(name, "p98"), snapshot.get98thPercentile(), timestamp, source, pointTags);
    wavefront.send(prefix(name, "p99"), snapshot.get99thPercentile(), timestamp, source, pointTags);
    wavefront.send(prefix(name, "p999"), snapshot.get999thPercentile(), timestamp, source, pointTags);
  }

  private void reportCounter(String name, Counter counter, long timestamp) throws IOException {
    wavefront.send(prefix(name, "count"), counter.getCount(), timestamp, source, pointTags);
  }

  private void reportGauge(String name, Gauge<Number> gauge, long timestamp) throws IOException {
    wavefront.send(prefix(name), gauge.getValue().doubleValue(), timestamp, source, pointTags);
  }

  private String prefix(String... components) {
    return MetricRegistry.name(prefix, components);
  }
}
