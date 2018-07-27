package com.wavefront.integrations.metrics;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.DeltaCounter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricAttribute;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.codahale.metrics.WavefrontHistogram;
import com.google.common.base.Preconditions;

import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.SafeFileDescriptorRatioGauge;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.tdunning.math.stats.Centroid;
import com.wavefront.common.HistogramGranularity;
import com.wavefront.common.MetricConstants;
import com.wavefront.common.MinuteBin;
import com.wavefront.common.Pair;
import com.wavefront.integrations.Wavefront;
import com.wavefront.integrations.WavefrontDirectSender;
import com.wavefront.integrations.WavefrontSender;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.validation.constraints.NotNull;

/**
 * A reporter which publishes metric values to a Wavefront Proxy from a Dropwizard {@link MetricRegistry}.
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
    private Map<String, String> pointTags;
    private boolean includeJvmMetrics;
    private Set<MetricAttribute> disabledMetricAttributes;
    private final Set<HistogramGranularity> histogramGranularities;

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
      this.disabledMetricAttributes = Collections.emptySet();
      this.histogramGranularities = new HashSet<>();
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
     * Report histogram distributions aggregated into minute intervals
     *
     * @return {@code this}
     */
    public Builder reportMinuteDistribution() {
      this.histogramGranularities.add(HistogramGranularity.MINUTE);
      return this;
    }

    /**
     * Report histogram distributions aggregated into hour intervals
     *
     * @return {@code this}
     */
    public Builder reportHourDistribution() {
      this.histogramGranularities.add(HistogramGranularity.HOUR);
      return this;
    }

    /**
     * Report histogram distributions aggregated into day intervals
     *
     * @return {@code this}
     */
    public Builder reportDayDistribution() {
      this.histogramGranularities.add(HistogramGranularity.DAY);
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
      int proxyMetricsPort;
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
          proxyMetricsPort = 2878;
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
            proxyMetricsPort = 2878;
          }
        } else {
          JSONObject details = jsonArray.getJSONObject(0);
          JSONObject credentials = details.getJSONObject("credentials");
          proxyHostname = credentials.getString("hostname");
          proxyMetricsPort = credentials.getInt("port");
        }
      }
      return new WavefrontReporter(registry,
          proxyHostname,
          proxyMetricsPort,
          clock,
          prefix,
          source,
          pointTags,
          rateUnit,
          durationUnit,
          filter,
          includeJvmMetrics,
          disabledMetricAttributes,
          histogramGranularities);
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
      return new WavefrontReporter(registry, wavefrontSender, clock, prefix, source, pointTags, rateUnit,
          durationUnit, filter, includeJvmMetrics, disabledMetricAttributes, histogramGranularities);
    }

    /**
     * Builds a {@link WavefrontReporter} with the given properties, sending metrics using the given
     * {@link WavefrontSender}.
     *
     * @param proxyHostname     Wavefront Proxy hostname.
     * @param proxyMetricsPort  Wavefront Proxy port for receiving metrics.
     * @return a {@link WavefrontReporter}
     */
    public WavefrontReporter build(String proxyHostname, int proxyMetricsPort) {
      return new WavefrontReporter(registry,
          proxyHostname,
          proxyMetricsPort,
          clock,
          prefix,
          source,
          pointTags,
          rateUnit,
          durationUnit,
          filter,
          includeJvmMetrics,
          disabledMetricAttributes,
          histogramGranularities);
    }

    /**
     * Builds a {@link WavefrontReporter} with the given properties, sending metrics using the given
     * {@link WavefrontSender}.
     *
     * @param proxyHostname         Wavefront Proxy hostname.
     * @param proxyMetricsPort      Wavefront Proxy port for receiving metrics.
     * @param proxyDistributionPort Wavefront Proxy port for receiving histogram distributions.
     * @return a {@link WavefrontReporter}
     */
    public WavefrontReporter build(String proxyHostname, int proxyMetricsPort, int proxyDistributionPort) {
      return new WavefrontReporter(registry,
          proxyHostname,
          proxyMetricsPort,
          proxyDistributionPort,
          clock,
          prefix,
          source,
          pointTags,
          rateUnit,
          durationUnit,
          filter,
          includeJvmMetrics,
          disabledMetricAttributes,
          histogramGranularities);
    }

    /**
     * Builds a {@link WavefrontReporter} with the given properties, sending metrics using the given
     * {@link WavefrontSender}.
     *
     * @param wavefrontSender   {@link WavefrontSender}.
     * @return a {@link WavefrontReporter}
     */
    public WavefrontReporter build(WavefrontSender wavefrontSender) {
      return new WavefrontReporter(registry,
              wavefrontSender,
              clock,
              prefix,
              source,
              pointTags,
              rateUnit,
              durationUnit,
              filter,
              includeJvmMetrics,
              disabledMetricAttributes,
              histogramGranularities);
    }
  }

  private final WavefrontSender wavefront;
  private final Clock clock;
  private final String prefix;
  private final String source;
  private final Map<String, String> pointTags;
  private final Set<HistogramGranularity> histogramGranularities;

  // indicates whether or not histogram distributions can be reported
  // true if (a) reporting directly or (b) reporting to proxy and a histogram port has been provided
  private final boolean reportWavefrontHistogram;

  private WavefrontReporter(MetricRegistry registry,
                            WavefrontSender wavefrontSender,
                            final Clock clock,
                            String prefix,
                            String source,
                            Map<String, String> pointTags,
                            TimeUnit rateUnit,
                            TimeUnit durationUnit,
                            MetricFilter filter,
                            boolean includeJvmMetrics,
                            Set<MetricAttribute> disabledMetricAttributes,
                            Set<HistogramGranularity> histogramGranularities) {
    super(registry, "wavefront-reporter", filter, rateUnit, durationUnit, Executors.newSingleThreadScheduledExecutor(),
        true, disabledMetricAttributes == null ? Collections.emptySet() : disabledMetricAttributes);
    this.wavefront = wavefrontSender;
    this.clock = clock;
    this.prefix = prefix;
    this.source = source;
    this.pointTags = pointTags;
    this.histogramGranularities = histogramGranularities;

    if (wavefrontSender instanceof WavefrontDirectSender) {
      this.reportWavefrontHistogram = true;
    } else if (wavefrontSender instanceof Wavefront && ((Wavefront) wavefrontSender).canHandleDistributions()) {
      this.reportWavefrontHistogram = true;
    } else {
      this.reportWavefrontHistogram = false;
    }

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
                            int proxyMetricsPort,
                            final Clock clock,
                            String prefix,
                            String source,
                            Map<String, String> pointTags,
                            TimeUnit rateUnit,
                            TimeUnit durationUnit,
                            MetricFilter filter,
                            boolean includeJvmMetrics,
                            Set<MetricAttribute> disabledMetricAttributes,
                            Set<HistogramGranularity> histogramGranularities) {
    this(registry, new Wavefront(proxyHostname, proxyMetricsPort), clock, prefix, source,
        pointTags, rateUnit, durationUnit, filter, includeJvmMetrics, disabledMetricAttributes, histogramGranularities);
  }

  private WavefrontReporter(MetricRegistry registry,
                            String proxyHostname,
                            int proxyMetricsPort,
                            int proxyDistributionPort,
                            final Clock clock,
                            String prefix,
                            String source,
                            Map<String, String> pointTags,
                            TimeUnit rateUnit,
                            TimeUnit durationUnit,
                            MetricFilter filter,
                            boolean includeJvmMetrics,
                            Set<MetricAttribute> disabledMetricAttributes,
                            Set<HistogramGranularity> histogramGranularities) {
    this(registry, new Wavefront(proxyHostname, proxyMetricsPort, proxyDistributionPort), clock, prefix, source,
        pointTags, rateUnit, durationUnit, filter, includeJvmMetrics, disabledMetricAttributes, histogramGranularities);
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
    final long time = clock.getTime() / 1000;
    sendIfEnabled(MetricAttribute.MAX, name, convertDuration(snapshot.getMax()), time);
    sendIfEnabled(MetricAttribute.MEAN, name, convertDuration(snapshot.getMean()), time);
    sendIfEnabled(MetricAttribute.MIN, name, convertDuration(snapshot.getMin()), time);
    sendIfEnabled(MetricAttribute.STDDEV, name, convertDuration(snapshot.getStdDev()), time);
    sendIfEnabled(MetricAttribute.P50, name, convertDuration(snapshot.getMedian()), time);
    sendIfEnabled(MetricAttribute.P75, name, convertDuration(snapshot.get75thPercentile()), time);
    sendIfEnabled(MetricAttribute.P95, name, convertDuration(snapshot.get95thPercentile()), time);
    sendIfEnabled(MetricAttribute.P98, name, convertDuration(snapshot.get98thPercentile()), time);
    sendIfEnabled(MetricAttribute.P99, name, convertDuration(snapshot.get99thPercentile()), time);
    sendIfEnabled(MetricAttribute.P999, name, convertDuration(snapshot.get999thPercentile()), time);

    reportMetered(name, timer);
  }

  private void reportMetered(String name, Metered meter) throws IOException {
    final long time = clock.getTime() / 1000;
    sendIfEnabled(MetricAttribute.COUNT, name, meter.getCount(), time);
    sendIfEnabled(MetricAttribute.M1_RATE, name, convertRate(meter.getOneMinuteRate()), time);
    sendIfEnabled(MetricAttribute.M5_RATE, name, convertRate(meter.getFiveMinuteRate()), time);
    sendIfEnabled(MetricAttribute.M15_RATE, name, convertRate(meter.getFifteenMinuteRate()), time);
    sendIfEnabled(MetricAttribute.MEAN_RATE, name, convertRate(meter.getMeanRate()), time);
  }

  private void reportHistogram(String name, Histogram histogram) throws IOException {
    if (histogram instanceof WavefrontHistogram) {
      // for (Dropwizard Metrics) WavefrontHistogram...
      if (reportWavefrontHistogram && !histogramGranularities.isEmpty()) {
        // ...if we're able to send a distribution and at least one aggregation interval has been specified,
        // then send as a distribution
        WavefrontHistogram wavefrontHistogram = (WavefrontHistogram) histogram;
        List<MinuteBin> bins = wavefrontHistogram.bins(true);
        if (bins.isEmpty()) return; // don't send empty histograms
        for (MinuteBin minuteBin : bins) {
          List<Pair<Double, Integer>> distribution = new ArrayList<>();
          for (Centroid c : minuteBin.getDist().centroids()) {
            distribution.add(new Pair<>(c.mean(), c.count()));
          }
          wavefront.send(histogramGranularities, minuteBin.getMinuteMillis() / 1000,
              distribution, prefixAndSanitize(name), source, pointTags);
        }
      } else if (!reportWavefrontHistogram) {
        // ...if we're unable to send a distribution, log an error message
        LOGGER.error("Unable to report WavefrontHistogram " + name + " because the histogram port is disabled on the proxy");
      } else {  // histogramGranularities.isEmpty()
        // ...if no aggregation intervals have been specified, no-op
        return;
      }
    } else {
      final Snapshot snapshot = histogram.getSnapshot();
      final long time = clock.getTime() / 1000;
      sendIfEnabled(MetricAttribute.COUNT, name, histogram.getCount(), time);
      sendIfEnabled(MetricAttribute.MAX, name, snapshot.getMax(), time);
      sendIfEnabled(MetricAttribute.MEAN, name, snapshot.getMean(), time);
      sendIfEnabled(MetricAttribute.MIN, name, snapshot.getMin(), time);
      sendIfEnabled(MetricAttribute.STDDEV, name, snapshot.getStdDev(), time);
      sendIfEnabled(MetricAttribute.P50, name, snapshot.getMedian(), time);
      sendIfEnabled(MetricAttribute.P75, name, snapshot.get75thPercentile(), time);
      sendIfEnabled(MetricAttribute.P95, name, snapshot.get95thPercentile(), time);
      sendIfEnabled(MetricAttribute.P98, name, snapshot.get98thPercentile(), time);
      sendIfEnabled(MetricAttribute.P99, name, snapshot.get99thPercentile(), time);
      sendIfEnabled(MetricAttribute.P999, name, snapshot.get999thPercentile(), time);
    }
  }

  private void reportCounter(String name, Counter counter) throws IOException {
    if (counter instanceof DeltaCounter) {
      long count = counter.getCount();
      name = MetricConstants.DELTA_PREFIX + prefixAndSanitize(name.substring(1), "count");
      wavefront.send(name, count,clock.getTime() / 1000, source, pointTags);
      counter.dec(count);
    } else {
      wavefront.send(prefixAndSanitize(name, "count"), counter.getCount(), clock.getTime() / 1000, source, pointTags);
    }
  }

  private void reportGauge(String name, Gauge<Number> gauge) throws IOException {
    wavefront.send(prefixAndSanitize(name), gauge.getValue().doubleValue(), clock.getTime() / 1000, source, pointTags);
  }

  private void sendIfEnabled(MetricAttribute type, String name, double value, long timestamp) throws IOException {
    if (!getDisabledMetricAttributes().contains(type)) {
      wavefront.send(prefixAndSanitize(name, type.getCode()), value, timestamp, source, pointTags);
    }
  }

  private String prefixAndSanitize(String... components) {
    return sanitize(MetricRegistry.name(prefix, components));
  }

  private static String sanitize(String name) {
    return SIMPLE_NAMES.matcher(name).replaceAll("_");
  }

  private static final Pattern SIMPLE_NAMES = Pattern.compile("[^a-zA-Z0-9_.\\-~]");
}
