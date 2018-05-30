package com.wavefront.integrations.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.metrics.jvm.ThreadStatesGaugeSet;
import com.wavefront.integrations.Wavefront;
import com.wavefront.integrations.WavefrontSender;

import io.dropwizard.metrics.Clock;
import io.dropwizard.metrics.Counter;
import io.dropwizard.metrics.Gauge;
import io.dropwizard.metrics.Histogram;
import io.dropwizard.metrics.Meter;
import io.dropwizard.metrics.Metered;
import io.dropwizard.metrics.MetricFilter;
import io.dropwizard.metrics.MetricName;
import io.dropwizard.metrics.MetricRegistry;
import io.dropwizard.metrics.ScheduledReporter;
import io.dropwizard.metrics.Snapshot;
import io.dropwizard.metrics.Timer;
import io.dropwizard.metrics.jvm.BufferPoolMetricSet;
import io.dropwizard.metrics.jvm.ClassLoadingGaugeSet;
import io.dropwizard.metrics.jvm.FileDescriptorRatioGauge;
import io.dropwizard.metrics.jvm.GarbageCollectorMetricSet;
import io.dropwizard.metrics.jvm.MemoryUsageGaugeSet;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * A reporter which publishes metric values to a Wavefront Proxy.
 *
 * This sends a DIFFERENT metrics taxonomy than the Wavefront "yammer" metrics reporter.
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
   * a host named "unknown", no point Tags, and not filtering metrics.
   */
  public static class Builder {
    private final MetricRegistry registry;
    private Clock clock;
    private String prefix;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;
    private String source;
    private Map<String, String> reporterPTags;
    private boolean includeJvmMetrics;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.clock = Clock.defaultClock();
      this.prefix = null;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
      this.source = "dropwizard-metrics";
      this.reporterPTags = new HashMap<String, String>();
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
     * @param metricPTags the metricPTags Map for all metrics
     * @return {@code this}
     */
    public Builder withPointTags(Map<String, String> reporterPTags) {
      this.reporterPTags.putAll(reporterPTags);
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
      this.reporterPTags.put(ptagK, ptagV);
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
                                   reporterPTags,
                                   rateUnit,
                                   durationUnit,
                                   filter,
                                   includeJvmMetrics);
    
  }

  /**
   * Builds a {@link WavefrontReporter} with the given properties, sending metrics using the
   * given {@link WavefrontSender}.
   *
   * @param Wavefront a {@link WavefrontSender}
   * @return a {@link WavefrontReporter}
   */
  public WavefrontReporter build(WavefrontSender wavefrontSender) {
    return new WavefrontReporter(registry,
            wavefrontSender,
            clock,
            prefix,
            source,
            reporterPTags,
            rateUnit,
            durationUnit,
            filter,
            includeJvmMetrics);
  }
}

  private static final Logger LOGGER = LoggerFactory.getLogger(WavefrontReporter.class);

  private final WavefrontSender wavefront;
  private final Clock clock;
  private final MetricName prefix;
  private final String source;
  private final Map<String, String> reporterPTags;

  private WavefrontReporter(MetricRegistry registry,
                            String proxyHostname,
                            int proxyPort,
                            final Clock clock,
                            String prefix,
                            String source,
                            Map<String, String> reporterPTags,
                            TimeUnit rateUnit,
                            TimeUnit durationUnit,
                            MetricFilter filter,
                            boolean includeJvmMetrics) {

    super(registry, "wavefront-reporter", filter, rateUnit, durationUnit);
    this.wavefront = new Wavefront(proxyHostname, proxyPort);
    this.clock = clock;
    this.prefix = MetricName.build(prefix);
    this.source = source;
    this.reporterPTags = reporterPTags;

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
      registry.register("jvm.fd_usage", new SafeFileDescriptorRatioGauge());
      registry.register("jvm.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
      registry.register("jvm.gc", new GarbageCollectorMetricSet());
      registry.register("jvm.memory", new MemoryUsageGaugeSet());
      registry.register("jvm.thread-states", new ThreadStatesGaugeSet());
    }
  }

  private WavefrontReporter(MetricRegistry registry,
                            WavefrontSender wavefrontSender
                            final Clock clock,
                            String prefix,
                            String source,
                            Map<String, String> reporterPTags,
                            TimeUnit rateUnit,
                            TimeUnit durationUnit,
                            MetricFilter filter,
                            boolean includeJvmMetrics) {

    super(registry, "wavefront-reporter", filter, rateUnit, durationUnit);
    this.wavefront = wavefrontSender;
    this.clock = clock;
    this.prefix = MetricName.build(prefix);
    this.source = source;
    this.reporterPTags = reporterPTags;

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
      registry.register("jvm.fd_usage", new SafeFileDescriptorRatioGauge());
      registry.register("jvm.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
      registry.register("jvm.gc", new GarbageCollectorMetricSet());
      registry.register("jvm.memory", new MemoryUsageGaugeSet());
      registry.register("jvm.thread-states", new ThreadStatesGaugeSet());
    }
  }

  @Override
  public void report(SortedMap<MetricName, Gauge> gauges,
                     SortedMap<MetricName, Counter> counters,
                     SortedMap<MetricName, Histogram> histograms,
                     SortedMap<MetricName, Meter> meters,
                     SortedMap<MetricName, Timer> timers) {
    final long timestamp = clock.getTime() / 1000;

    try {
      if (!wavefront.isConnected()) {
        wavefront.connect();
      }

      for (Entry<MetricName, Gauge> entry : gauges.entrySet()) {
        if (entry.getValue().getValue() instanceof Number) {
          reportGauge(entry.getKey(), entry.getValue(), timestamp, combineTags(reporterPTags, entry));
        }
      }

      for (Map.Entry<MetricName, Counter> entry : counters.entrySet()) {
        reportCounter(entry.getKey(), entry.getValue(), timestamp, combineTags(reporterPTags, entry));
      }

      for (Map.Entry<MetricName, Histogram> entry : histograms.entrySet()) {
        reportHistogram(entry.getKey(), entry.getValue(), timestamp, combineTags(reporterPTags, entry));
      }

      for (Map.Entry<MetricName, Meter> entry : meters.entrySet()) {
        reportMetered(entry.getKey(), entry.getValue(), timestamp, combineTags(reporterPTags, entry));
      }

      for (Map.Entry<MetricName, Timer> entry : timers.entrySet()) {
        reportTimer(entry.getKey(), entry.getValue(), timestamp, combineTags(reporterPTags, entry));
      }

      wavefront.flush();
    } catch (IOException e) {
      LOGGER.warn("Unable to report to Wavefront", wavefront, e);
      try {
        wavefront.close();
      } catch (IOException e1) {
        LOGGER.warn("Error closing Wavefront", wavefront, e1);
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

  private void reportTimer(MetricName name, Timer timer, long timestamp, Map<String, String> combinedTags) throws IOException {
    final Snapshot snapshot = timer.getSnapshot();

    wavefront.send(prefix(name, "max"), convertDuration(snapshot.getMax()), timestamp, source, combinedTags);
    wavefront.send(prefix(name, "mean"), convertDuration(snapshot.getMean()), timestamp, source, combinedTags);
    wavefront.send(prefix(name, "min"), convertDuration(snapshot.getMin()), timestamp, source, combinedTags);
    wavefront.send(prefix(name, "stddev"),
                   convertDuration(snapshot.getStdDev()),
                   timestamp, source, combinedTags);
    wavefront.send(prefix(name, "p50"),
                   convertDuration(snapshot.getMedian()),
                   timestamp, source, combinedTags);
    wavefront.send(prefix(name, "p75"),
                   convertDuration(snapshot.get75thPercentile()),
                   timestamp, source, combinedTags);
    wavefront.send(prefix(name, "p95"),
                   convertDuration(snapshot.get95thPercentile()),
                   timestamp, source, combinedTags);
    wavefront.send(prefix(name, "p98"),
                   convertDuration(snapshot.get98thPercentile()),
                   timestamp, source, combinedTags);
    wavefront.send(prefix(name, "p99"),
                   convertDuration(snapshot.get99thPercentile()),
                   timestamp, source, combinedTags);
    wavefront.send(prefix(name, "p999"),
                   convertDuration(snapshot.get999thPercentile()),
                   timestamp, source, combinedTags);

    reportMetered(name, timer, timestamp, combinedTags);
  }

  private void reportMetered(MetricName name, Metered meter, long timestamp, Map<String, String> combinedTags) throws IOException {
    wavefront.send(prefix(name, "count"), meter.getCount(), timestamp, source, combinedTags);
    wavefront.send(prefix(name, "m1_rate"),
                   convertRate(meter.getOneMinuteRate()),
                   timestamp, source, combinedTags);
    wavefront.send(prefix(name, "m5_rate"),
                   convertRate(meter.getFiveMinuteRate()),
                   timestamp, source, combinedTags);
    wavefront.send(prefix(name, "m15_rate"),
                   convertRate(meter.getFifteenMinuteRate()),
                   timestamp, source, combinedTags);
    wavefront.send(prefix(name, "mean_rate"),
                   convertRate(meter.getMeanRate()),
                   timestamp, source, combinedTags);
  }

  private void reportHistogram(MetricName name, Histogram histogram, long timestamp, Map<String, String> combinedTags) throws IOException {
    final Snapshot snapshot = histogram.getSnapshot();
    wavefront.send(prefix(name, "count"), histogram.getCount(), timestamp, source, combinedTags);
    wavefront.send(prefix(name, "max"), snapshot.getMax(), timestamp, source, combinedTags);
    wavefront.send(prefix(name, "mean"), snapshot.getMean(), timestamp, source, combinedTags);
    wavefront.send(prefix(name, "min"), snapshot.getMin(), timestamp, source, combinedTags);
    wavefront.send(prefix(name, "stddev"), snapshot.getStdDev(), timestamp, source, combinedTags);
    wavefront.send(prefix(name, "p50"), snapshot.getMedian(), timestamp, source, combinedTags);
    wavefront.send(prefix(name, "p75"), snapshot.get75thPercentile(), timestamp, source, combinedTags);
    wavefront.send(prefix(name, "p95"), snapshot.get95thPercentile(), timestamp, source, combinedTags);
    wavefront.send(prefix(name, "p98"), snapshot.get98thPercentile(), timestamp, source, combinedTags);
    wavefront.send(prefix(name, "p99"), snapshot.get99thPercentile(), timestamp, source, combinedTags);
    wavefront.send(prefix(name, "p999"), snapshot.get999thPercentile(), timestamp, source, combinedTags);
  }

  private void reportCounter(MetricName name, Counter counter, long timestamp, Map<String, String> combinedTags) throws IOException {
    wavefront.send(prefix(name, "count"), counter.getCount(), timestamp, source, combinedTags);
  }

  private void reportGauge(MetricName name, Gauge<Number> gauge, long timestamp, Map<String, String> combinedTags) throws IOException {
    wavefront.send(prefix(name), gauge.getValue().doubleValue(), timestamp, source, combinedTags);
  }

  private String prefix(MetricName name, String... components) {
    return MetricName.join(MetricName.join(prefix, name), MetricName.build(components)).getKey();
  }

  private Map<String, String> combineTags(Map<String, String> rTags, Map.Entry<MetricName, ?> entry) {
    Map<String, String> combinedTags = new HashMap<String, String>();
    combinedTags.putAll(rTags);
    combinedTags.putAll(entry.getKey().getTags());
    return combinedTags;
  }
}
