package com.wavefront;

import com.google.common.collect.Maps;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.wavefront.integrations.WavefrontSender;
import com.wavefront.integrations.metrics.WavefrontReporter;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

/**
 * Wavefront reporter for your Dropwizard Application.
 * Will report metrics and histograms out of the box for you.
 *
 * @author Sushant Dewan (sushant@wavefront.com).
 */
public class WavefrontDropwizardApplicationReporter implements DropwizardApplicationReporter {

  /**
   * Use dedicated com.codahale.metrics.MetricRegistry to register all metrics/histograms
   */
  private final MetricRegistry metricRegistry = new MetricRegistry();

  /**
   * Quick cache look up of a counter for a given counter metric name
   */
  private final LoadingCache<String, Counter> counters = Caffeine.newBuilder().
      build(key -> metricRegistry.counter(key));

  /**
   * Quick cache look up of a WavefrontHistogram for a given histogram name
   */
  // TODO: Use Yammer histogram for now and replace it with wavefrontHistogram
  private final LoadingCache<String, Histogram> histograms = Caffeine.newBuilder().
      build(key -> metricRegistry.histogram(key));

  private static final String METRIC_PREFIX = "dropwizard.api";

  public static class Builder {
    // Required parameters below
    /**
     * Mechanism (via proxy or direct ingestion) to send metrics/histograms
     * from your app to Wavefront
     */
    private final WavefrontSender wavefrontSender;

    // Optional parameters below
    /**
     * How often do you want to report the metrics/histograms to Wavefront
     */
    private int reportingIntervalSeconds = 60;

    /**
     * Name of the application (e.g. "OrderingApplication").
     * This is a required field (defaults to "defaultApplication")
     */
    private String application = "defaultApplication";

    /**
     * Cluster name where the service is running.
     * Optional and null if not set and never reported to Wavefront.
     */
    @Nullable
    private String cluster;

    /**
     * Name of the service (e.g. "InventoryService") for the ordering app.
     * This is a required field (defaults to "defaultService")
     */
    private String service = "defaultService";

    /**
     * Optional shard information on which the service is running.
     * Null if not set and never reported to Wavefront.
     */
    @Nullable
    private String shard;

    Builder(WavefrontSender wavefrontSender) {
      this.wavefrontSender = wavefrontSender;
    }

    public Builder reportingIntervalSeconds(int reportingIntervalSeconds) {
      this.reportingIntervalSeconds = reportingIntervalSeconds;
      return this;
    }

    public Builder application(String application) {
      this.application = application;
      return this;
    }

    public Builder cluster(String cluster) {
      this.cluster = cluster;
      return this;
    }

    public Builder service(String service) {
      this.service = service;
      return this;
    }

    public Builder shard(String shard) {
      this.shard = shard;
      return this;
    }

    public WavefrontDropwizardApplicationReporter build() {
      return new WavefrontDropwizardApplicationReporter(this);
    }
  }

  private WavefrontDropwizardApplicationReporter(Builder builder) {
    String source;
    try {
      source = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      // Should never happen
      source = "unknown";
    }

    Map<String, String> pointTags = Maps.newHashMap();
    pointTags.put("application", builder.application);
    if (builder.cluster != null) {
      pointTags.put("cluster", builder.cluster);
    }
    pointTags.put("service", builder.service);
    if (builder.shard != null) {
      pointTags.put("shard", builder.shard);
    }

    WavefrontReporter wfReporter = WavefrontReporter.forRegistry(metricRegistry).
        withSource(source).withPointTags(pointTags).prefixedWith(METRIC_PREFIX).
        build(builder.wavefrontSender);
    wfReporter.start(builder.reportingIntervalSeconds, TimeUnit.SECONDS);
  }

  @Override
  public void updateHistogram(String metricName, long latencyMillis) {
    // TODO: use the WavefrontReporter to update WavefrontHistogram
    histograms.get(metricName).update(latencyMillis);
  }

  @Override
  public void incrementCounter(String metricName) {
    counters.get(metricName).inc();
  }
}
