package com.wavefront.integrations.metrics;

import com.google.common.base.Preconditions;
import com.wavefront.common.MetricConstants;
import com.wavefront.integrations.Wavefront;
import com.wavefront.integrations.WavefrontDirectSender;
import com.wavefront.integrations.WavefrontSender;
import io.dropwizard.metrics5.ScheduledReporter;
import io.dropwizard.metrics5.MetricRegistry;
import io.dropwizard.metrics5.Clock;
import io.dropwizard.metrics5.MetricFilter;
import io.dropwizard.metrics5.MetricAttribute;
import io.dropwizard.metrics5.Timer;
import io.dropwizard.metrics5.Gauge;
import io.dropwizard.metrics5.MetricName;
import io.dropwizard.metrics5.Counter;
import io.dropwizard.metrics5.Histogram;
import io.dropwizard.metrics5.Snapshot;
import io.dropwizard.metrics5.Meter;
import io.dropwizard.metrics5.Metered;
import io.dropwizard.metrics5.DeltaCounter;
import io.dropwizard.metrics5.jvm.ClassLoadingGaugeSet;
import io.dropwizard.metrics5.jvm.SafeFileDescriptorRatioGauge;
import io.dropwizard.metrics5.jvm.BufferPoolMetricSet;
import io.dropwizard.metrics5.jvm.GarbageCollectorMetricSet;
import io.dropwizard.metrics5.jvm.MemoryUsageGaugeSet;
import io.dropwizard.metrics5.jvm.ThreadStatesGaugeSet;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.Map;
import java.util.Collections;
import java.util.SortedMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * A reporter which publishes metric values to a Wavefront Proxy from a Dropwizard {@link MetricRegistry}.
 * This reporter is based on Dropwizard version 5.0.0-rc2 that has native support for tags. This reporter
 * leverages the tags maintained as part of the MetricName object that is registered with the metric registry
 * for all metrics types(Counter, Gauge, Histogram, Meter, Timer)
 *
 * @author Subramaniam Narayanan
 */
public class WavefrontReporter extends ScheduledReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(WavefrontReporter.class);
  
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
    private boolean includeJvmMetrics;
    private Set<MetricAttribute> disabledMetricAttributes;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.clock = Clock.defaultClock();
      this.prefix = null;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
      this.source = "dropwizard-metrics";
      this.includeJvmMetrics = false;
      this.disabledMetricAttributes = Collections.emptySet();
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
     * Don't report the passed metric attributes for all metrics (e.g. "p999", "stddev" or "m15").
     * See {@link MetricAttribute}.
     *
     * @param disabledMetricAttributes a set of {@link MetricAttribute}
     * @return {@code this}
     */
    public Builder disabledMetricAttributes(Set<MetricAttribute> disabledMetricAttributes) {
      this.disabledMetricAttributes = disabledMetricAttributes;
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
          rateUnit,
          durationUnit,
          filter,
          includeJvmMetrics,
          disabledMetricAttributes);
    }

    /**
     * Builds a {@link WavefrontReporter} with the given properties, sending metrics directly
     * to a given Wavefront server using direct ingestion APIs.
     *
     * @param server Wavefront server hostname of the form "https://serverName.wavefront.com"
     * @param token  Wavefront API token with direct ingestion permission
     * @return a {@link WavefrontReporter}
     */
    public WavefrontReporter buildDirect(String server, String token) {
      WavefrontSender wavefrontSender = new WavefrontDirectSender(server, token);
      return new WavefrontReporter(registry, wavefrontSender, clock, prefix, source, rateUnit,
          durationUnit, filter, includeJvmMetrics, disabledMetricAttributes);
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
          rateUnit,
          durationUnit,
          filter,
          includeJvmMetrics,
          disabledMetricAttributes);
    }

  /**
   * Builds a {@link WavefrontReporter} with the given properties, sending metrics using the given
   * {@link WavefrontSender}.
   *
   * @param wavefrontSender a {@link WavefrontSender}.
   * @return a {@link WavefrontReporter}
   */
  public WavefrontReporter build(WavefrontSender wavefrontSender) {
    return new WavefrontReporter(registry,
            wavefrontSender,
            clock,
            prefix,
            source,
            rateUnit,
            durationUnit,
            filter,
            includeJvmMetrics,
            disabledMetricAttributes);
  }
}

  private final WavefrontSender wavefront;
  private final Clock clock;
  private final String prefix;
  private final String source;

  private WavefrontReporter(MetricRegistry registry,
                            WavefrontSender wavefrontSender,
                            final Clock clock,
                            String prefix,
                            String source,
                            TimeUnit rateUnit,
                            TimeUnit durationUnit,
                            MetricFilter filter,
                            boolean includeJvmMetrics,
                            Set<MetricAttribute> disabledMetricAttributes) {
    super(registry, "wavefront-reporter", filter, rateUnit, durationUnit, Executors.newSingleThreadScheduledExecutor(),
        true, disabledMetricAttributes == null ? Collections.emptySet() : disabledMetricAttributes);
    this.wavefront = wavefrontSender;
    this.clock = clock;
    this.prefix = prefix;
    this.source = source;

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

  private WavefrontReporter(MetricRegistry registry,
                            String proxyHostname,
                            int proxyPort,
                            final Clock clock,
                            String prefix,
                            String source,
                            TimeUnit rateUnit,
                            TimeUnit durationUnit,
                            MetricFilter filter,
                            boolean includeJvmMetrics,
                            Set<MetricAttribute> disabledMetricAttributes) {
    this(registry, new Wavefront(proxyHostname, proxyPort), clock, prefix, source, rateUnit,
        durationUnit, filter, includeJvmMetrics, disabledMetricAttributes);
  }

  /**
   * Called periodically by the polling thread. Subclasses should report all the given metrics.
   *
   * @param gauges     all of the gauges in the registry
   * @param counters   all of the counters in the registry
   * @param histograms all of the histograms in the registry
   * @param meters     all of the meters in the registry
   * @param timers     all of the timers in the registry
   */
  @Override
  @SuppressWarnings("rawtypes")
  public void report(SortedMap<MetricName, Gauge> gauges,
                              SortedMap<MetricName, Counter> counters,
                              SortedMap<MetricName, Histogram> histograms,
                              SortedMap<MetricName, Meter> meters,
                              SortedMap<MetricName, Timer> timers) {
    try {
      if (!wavefront.isConnected()) {
        wavefront.connect();
      }

      for (Map.Entry<MetricName, Gauge> entry : gauges.entrySet()) {
        if (entry.getValue().getValue() instanceof Number) {
          reportGauge(entry.getKey(), entry.getValue());
        }
      }

      for (Map.Entry<MetricName, Counter> entry : counters.entrySet()) {
        reportCounter(entry.getKey(), entry.getValue());
      }

      for (Map.Entry<MetricName, Histogram> entry : histograms.entrySet()) {
        reportHistogram(entry.getKey(), entry.getValue());
      }

      for (Map.Entry<MetricName, Meter> entry : meters.entrySet()) {
        reportMetered(entry.getKey(), entry.getValue());
      }

      for (Map.Entry<MetricName, Timer> entry : timers.entrySet()) {
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

  private void reportTimer(MetricName metricName, Timer timer) throws IOException {
    final Snapshot snapshot = timer.getSnapshot();
    final long time = clock.getTime() / 1000;
    sendIfEnabled(MetricAttribute.MAX, metricName, convertDuration(snapshot.getMax()), time);
    sendIfEnabled(MetricAttribute.MEAN, metricName, convertDuration(snapshot.getMean()), time);
    sendIfEnabled(MetricAttribute.MIN, metricName, convertDuration(snapshot.getMin()), time);
    sendIfEnabled(MetricAttribute.STDDEV, metricName, convertDuration(snapshot.getStdDev()), time);
    sendIfEnabled(MetricAttribute.P50, metricName, convertDuration(snapshot.getMedian()), time);
    sendIfEnabled(MetricAttribute.P75, metricName, convertDuration(snapshot.get75thPercentile()), time);
    sendIfEnabled(MetricAttribute.P95, metricName, convertDuration(snapshot.get95thPercentile()), time);
    sendIfEnabled(MetricAttribute.P98, metricName, convertDuration(snapshot.get98thPercentile()), time);
    sendIfEnabled(MetricAttribute.P99, metricName, convertDuration(snapshot.get99thPercentile()), time);
    sendIfEnabled(MetricAttribute.P999, metricName, convertDuration(snapshot.get999thPercentile()), time);

    reportMetered(metricName, timer);
  }

  private void reportMetered(MetricName metricName, Metered meter) throws IOException {
    final long time = clock.getTime() / 1000;
    sendIfEnabled(MetricAttribute.COUNT, metricName, meter.getCount(), time);
    sendIfEnabled(MetricAttribute.M1_RATE, metricName, convertRate(meter.getOneMinuteRate()), time);
    sendIfEnabled(MetricAttribute.M5_RATE, metricName, convertRate(meter.getFiveMinuteRate()), time);
    sendIfEnabled(MetricAttribute.M15_RATE, metricName, convertRate(meter.getFifteenMinuteRate()), time);
    sendIfEnabled(MetricAttribute.MEAN_RATE, metricName, convertRate(meter.getMeanRate()), time);
  }

  private void reportHistogram(MetricName metricName, Histogram histogram) throws IOException {
    final Snapshot snapshot = histogram.getSnapshot();
    final long time = clock.getTime() / 1000;
    sendIfEnabled(MetricAttribute.COUNT, metricName, histogram.getCount(), time);
    sendIfEnabled(MetricAttribute.MAX, metricName, snapshot.getMax(), time);
    sendIfEnabled(MetricAttribute.MEAN, metricName, snapshot.getMean(), time);
    sendIfEnabled(MetricAttribute.MIN, metricName, snapshot.getMin(), time);
    sendIfEnabled(MetricAttribute.STDDEV, metricName, snapshot.getStdDev(), time);
    sendIfEnabled(MetricAttribute.P50, metricName, snapshot.getMedian(), time);
    sendIfEnabled(MetricAttribute.P75, metricName, snapshot.get75thPercentile(), time);
    sendIfEnabled(MetricAttribute.P95, metricName, snapshot.get95thPercentile(), time);
    sendIfEnabled(MetricAttribute.P98, metricName, snapshot.get98thPercentile(), time);
    sendIfEnabled(MetricAttribute.P99, metricName, snapshot.get99thPercentile(), time);
    sendIfEnabled(MetricAttribute.P999, metricName, snapshot.get999thPercentile(), time);
  }

  private void reportCounter(MetricName metricName, Counter counter) throws IOException {
    if (counter instanceof DeltaCounter) {
      long count = counter.getCount();
      String name = MetricConstants.DELTA_PREFIX + prefixAndSanitize(metricName.getKey().substring(1), "count");
      wavefront.send(name, count,clock.getTime() / 1000, source, metricName.getTags());
      counter.dec(count);
    } else {
      wavefront.send(prefixAndSanitize(metricName.getKey(), "count"), counter.getCount(), clock.getTime() / 1000, source, metricName.getTags());
    }
  }

  private void reportGauge(MetricName metricName, Gauge<Number> gauge) throws IOException {
    wavefront.send(prefixAndSanitize(metricName.getKey()), gauge.getValue().doubleValue(), clock.getTime() / 1000, source, metricName.getTags());
  }

  private void sendIfEnabled(MetricAttribute type, MetricName metricName, double value, long timestamp) throws IOException {
    if (!getDisabledMetricAttributes().contains(type)) {
      wavefront.send(prefixAndSanitize(metricName.getKey(), type.getCode()), value, timestamp, source, metricName.getTags());
    }
  }

  private String prefixAndSanitize(String... components) {
    return sanitize(MetricRegistry.name(prefix, components).getKey());
  }

  private static String sanitize(String name) {
    return SIMPLE_NAMES.matcher(name).replaceAll("_");
  }

  private static final Pattern SIMPLE_NAMES = Pattern.compile("[^a-zA-Z0-9_.\\-~]");
}
