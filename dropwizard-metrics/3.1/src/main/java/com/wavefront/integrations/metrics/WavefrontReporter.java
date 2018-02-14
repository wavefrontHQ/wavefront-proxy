package com.wavefront.integrations.metrics;

import com.google.common.base.Preconditions;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.SafeFileDescriptorRatioGauge;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.wavefront.integrations.Wavefront;
import com.wavefront.integrations.WavefrontSender;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;

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
   * default clock, converting rates to events/second, converting durations to milliseconds, a host
   * named "unknown", no point Tags, and not filtering any metrics.
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
      this.pointTags = new HashMap<>();
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
     * @return {@code this}
     */
    public Builder withJvmMetrics() {
      this.includeJvmMetrics = true;
      return this;
    }

    /**
     * Builds a {@link WavefrontReporter} from the VCAP_SERVICES env variable, sending metrics
     * using the given {@link WavefrontSender}. This should be used in PCF environment only. It
     * uses 'wavefront-proxy' as the name to fetch the proxy details from VCAP_SERVICES.
     *
     * @return a {@link WavefrontReporter}
     */
    public WavefrontReporter bindToCloudFoundryService() {
      return bindToCloudFoundryService("wavefront-proxy", false);
    }

    /**
     * Builds a {@link WavefrontReporter} from the VCAP_SERVICES env variable, sending metrics
     * using the given {@link WavefrontSender}. This should be used in PCF environment only. It
     * assumes failOnError to be false.
     *
     * @return a {@link WavefrontReporter}
     */
    public WavefrontReporter bindToCloudFoundryService(@NotNull String proxyServiceName) {
      return bindToCloudFoundryService(proxyServiceName, false);
    }

    /**
     * Builds a {@link WavefrontReporter} from the VCAP_SERVICES env variable, sending metrics
     * using the given {@link WavefrontSender}. This should be used in PCF environment only.
     *
     * @param proxyServiceName The name of the wavefront proxy service. If wavefront-tile is used to
     *                         deploy the proxy, then the service name will be 'wavefront-proxy'.
     * @param failOnError      A flag to determine what to do if the service parameters are not
     *                         available. If 'true' then the method will throw RuntimeException else
     *                         it will log an error message and continue.
     * @return a {@link WavefrontReporter}
     */
    public WavefrontReporter bindToCloudFoundryService(@NotNull String proxyServiceName,
                                                       boolean failOnError) {

      Preconditions.checkNotNull(proxyServiceName, "proxyServiceName arg should not be null");

      String proxyHostname;
      int proxyPort;
      // read the env variable VCAP_SERVICES
      String services = System.getenv("VCAP_SERVICES");
      if (services == null || services.length() == 0) {
        if (failOnError) {
          throw new RuntimeException("VCAP_SERVICES environment variable is unavailable.");
        } else {
          LOGGER.error("Environment variable VCAP_SERVICES is empty. No metrics will be reported " +
              "to wavefront proxy.");
          // since the wavefront-proxy is not tied to the app, use dummy hostname and port.
          proxyHostname = "";
          proxyPort = 2878;
        }
      } else {
        // parse the json to read the hostname and port
        JSONObject json = new JSONObject(services);
        // When wavefront tile is installed on PCF, it will be automatically named wavefront-proxy
        JSONArray jsonArray = json.getJSONArray(proxyServiceName);
        if (jsonArray == null || jsonArray.isNull(0)) {
          if (failOnError) {
            throw new RuntimeException(proxyServiceName + " is not present in the VCAP_SERVICES " +
                "env variable. Please verify and provide the wavefront proxy service name.");
          } else {
            LOGGER.error(proxyServiceName + " is not present in VCAP_SERVICES env variable. No " +
                "metrics will be reported to wavefront proxy.");
            // since the wavefront-proxy is not tied to the app, use dummy hostname and port.
            proxyHostname = "";
            proxyPort = 2878;
          }
        } else {
          JSONObject details = jsonArray.getJSONObject(0);
          JSONObject credentials = details.getJSONObject("credentials");
          proxyHostname = credentials.getString("hostname");
          proxyPort = credentials.getInt("port");
        }
      }
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

    /**
     * Builds a {@link WavefrontReporter} with the given properties, sending metrics using the given
     * {@link WavefrontSender}.
     *
     * @param proxyHostname Wavefront Proxy hostname.
     * @param proxyPort     Wavefront Proxy port.
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
      registry.register("jvm.uptime", (Gauge<Long>) () -> ManagementFactory.getRuntimeMXBean().getUptime());
      registry.register("jvm.current_time", (Gauge<Long>) clock::getTime);
      registry.register("jvm.classes", new ClassLoadingGaugeSet());
      registry.register("jvm.fd_usage", new SafeFileDescriptorRatioGauge());
      registry.register("jvm.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
      registry.register("jvm.gc", new GarbageCollectorMetricSet());
      registry.register("jvm.memory", new MemoryUsageGaugeSet());
      registry.register("jvm.thread-states", new ThreadStatesGaugeSet());
    }
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges,
                     SortedMap<String, Counter> counters,
                     SortedMap<String, Histogram> histograms,
                     SortedMap<String, Meter> meters,
                     SortedMap<String, Timer> timers) {

    try {
      if (!wavefront.isConnected()) {
        wavefront.connect();
      }

      for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
        if (entry.getValue().getValue() instanceof Number) {
          reportGauge(entry.getKey(), entry.getValue());
        }
      }

      for (Map.Entry<String, Counter> entry : counters.entrySet()) {
        reportCounter(entry.getKey(), entry.getValue());
      }

      for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
        reportHistogram(entry.getKey(), entry.getValue());
      }

      for (Map.Entry<String, Meter> entry : meters.entrySet()) {
        reportMetered(entry.getKey(), entry.getValue());
      }

      for (Map.Entry<String, Timer> entry : timers.entrySet()) {
        reportTimer(entry.getKey(), entry.getValue());
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

  private void reportTimer(String name, Timer timer) throws IOException {
    final Snapshot snapshot = timer.getSnapshot();

    wavefront.send(prefix(name, "max"), convertDuration(snapshot.getMax()), source, pointTags);
    wavefront.send(prefix(name, "mean"), convertDuration(snapshot.getMean()), source, pointTags);
    wavefront.send(prefix(name, "min"), convertDuration(snapshot.getMin()), source, pointTags);
    wavefront.send(prefix(name, "stddev"),
        convertDuration(snapshot.getStdDev()),
        source, pointTags);
    wavefront.send(prefix(name, "p50"),
        convertDuration(snapshot.getMedian()),
        source, pointTags);
    wavefront.send(prefix(name, "p75"),
        convertDuration(snapshot.get75thPercentile()),
        source, pointTags);
    wavefront.send(prefix(name, "p95"),
        convertDuration(snapshot.get95thPercentile()),
        source, pointTags);
    wavefront.send(prefix(name, "p98"),
        convertDuration(snapshot.get98thPercentile()),
        source, pointTags);
    wavefront.send(prefix(name, "p99"),
        convertDuration(snapshot.get99thPercentile()),
        source, pointTags);
    wavefront.send(prefix(name, "p999"),
        convertDuration(snapshot.get999thPercentile()),
        source, pointTags);

    reportMetered(name, timer);
  }

  private void reportMetered(String name, Metered meter) throws IOException {
    wavefront.send(prefix(name, "count"), meter.getCount(), source, pointTags);
    wavefront.send(prefix(name, "m1_rate"),
        convertRate(meter.getOneMinuteRate()),
        source, pointTags);
    wavefront.send(prefix(name, "m5_rate"),
        convertRate(meter.getFiveMinuteRate()),
        source, pointTags);
    wavefront.send(prefix(name, "m15_rate"),
        convertRate(meter.getFifteenMinuteRate()),
        source, pointTags);
    wavefront.send(prefix(name, "mean_rate"),
        convertRate(meter.getMeanRate()),
        source, pointTags);
  }

  private void reportHistogram(String name, Histogram histogram) throws IOException {
    final Snapshot snapshot = histogram.getSnapshot();
    wavefront.send(prefix(name, "count"), histogram.getCount(), source, pointTags);
    wavefront.send(prefix(name, "max"), snapshot.getMax(), source, pointTags);
    wavefront.send(prefix(name, "mean"), snapshot.getMean(), source, pointTags);
    wavefront.send(prefix(name, "min"), snapshot.getMin(), source, pointTags);
    wavefront.send(prefix(name, "stddev"), snapshot.getStdDev(), source, pointTags);
    wavefront.send(prefix(name, "p50"), snapshot.getMedian(), source, pointTags);
    wavefront.send(prefix(name, "p75"), snapshot.get75thPercentile(), source, pointTags);
    wavefront.send(prefix(name, "p95"), snapshot.get95thPercentile(), source, pointTags);
    wavefront.send(prefix(name, "p98"), snapshot.get98thPercentile(), source, pointTags);
    wavefront.send(prefix(name, "p99"), snapshot.get99thPercentile(), source, pointTags);
    wavefront.send(prefix(name, "p999"), snapshot.get999thPercentile(), source, pointTags);
  }

  private void reportCounter(String name, Counter counter) throws IOException {
    wavefront.send(prefix(name, "count"), counter.getCount(), source, pointTags);
  }

  private void reportGauge(String name, Gauge<Number> gauge) throws IOException {
    wavefront.send(prefix(name), gauge.getValue().doubleValue(), source, pointTags);
  }

  private String prefix(String... components) {
    return MetricRegistry.name(prefix, components);
  }
}
