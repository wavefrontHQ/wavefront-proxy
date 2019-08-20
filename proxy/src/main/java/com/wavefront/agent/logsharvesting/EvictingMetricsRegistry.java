package com.wavefront.agent.logsharvesting;

import com.google.common.collect.Sets;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Ticker;
import com.wavefront.agent.config.MetricMatcher;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.DeltaCounter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.WavefrontHistogram;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Wrapper for a Yammer {@link com.yammer.metrics.core.MetricsRegistry}, but has extra features
 * regarding automatic removal of metrics.
 *
 * With the introduction of Delta Counter for Yammer metrics, this class now treats Counters as
 * Delta Counters. So anybody using this {@link #getCounter(MetricName, MetricMatcher)} method
 * will get an instance of Delta counter.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class EvictingMetricsRegistry {
  protected static final Logger logger = Logger.getLogger(EvictingMetricsRegistry.class.getCanonicalName());
  private final MetricsRegistry metricsRegistry;
  private final Cache<MetricName, Metric> metricCache;
  private final LoadingCache<MetricMatcher, Set<MetricName>> metricNamesForMetricMatchers;
  private final boolean wavefrontHistograms;
  private final boolean useDeltaCounters;
  private final Supplier<Long> nowMillis;

  EvictingMetricsRegistry(MetricsRegistry metricRegistry, long expiryMillis,
                          boolean wavefrontHistograms, boolean useDeltaCounters,
                          Supplier<Long> nowMillis, Ticker ticker) {
    this.metricsRegistry = metricRegistry;
    this.nowMillis = nowMillis;
    this.wavefrontHistograms = wavefrontHistograms;
    this.useDeltaCounters = useDeltaCounters;
    this.metricCache = Caffeine.newBuilder()
        .expireAfterAccess(expiryMillis, TimeUnit.MILLISECONDS)
        .ticker(ticker)
        .writer(new CacheWriter<MetricName, Metric>() {
          @Override
          public void write(@Nonnull MetricName key, @Nonnull Metric value) {
          }

          @Override
          public void delete(@Nonnull MetricName key, @Nullable Metric value,
                             @Nonnull RemovalCause cause) {
            if (cause == RemovalCause.EXPIRED &&
                metricsRegistry.allMetrics().get(key) == value) {
              metricsRegistry.removeMetric(key);
            }
          }
        })
        .build();
    this.metricNamesForMetricMatchers = Caffeine.<MetricMatcher, Set<MetricName>>newBuilder()
        .build((metricMatcher) -> Sets.newHashSet());
  }

  public Counter getCounter(MetricName metricName, MetricMatcher metricMatcher) {
    if (useDeltaCounters) {
      // use delta counters instead of regular counters. It helps with load balancers present in
      // front of proxy (PUB-125)
      MetricName newMetricName = DeltaCounter.getDeltaCounterMetricName(metricName);
      metricNamesForMetricMatchers.get(metricMatcher).add(newMetricName);
      return (Counter) metricCache.get(newMetricName, key -> DeltaCounter.get(metricsRegistry,
          newMetricName));
    } else {
      metricNamesForMetricMatchers.get(metricMatcher).add(metricName);
      return (Counter) metricCache.get(metricName, metricsRegistry::newCounter);
    }
  }

  public Gauge getGauge(MetricName metricName, MetricMatcher metricMatcher) {
    metricNamesForMetricMatchers.get(metricMatcher).add(metricName);
    return (Gauge) metricCache.get(
        metricName, (key) -> metricsRegistry.newGauge(key, new ChangeableGauge<Double>()));
  }

  public Histogram getHistogram(MetricName metricName, MetricMatcher metricMatcher) {
    metricNamesForMetricMatchers.get(metricMatcher).add(metricName);
    return (Histogram) metricCache.get(
        metricName,
        (key) -> wavefrontHistograms
            ? WavefrontHistogram.get(metricsRegistry, key, this.nowMillis)
            : metricsRegistry.newHistogram(metricName, false));
  }

  public synchronized void evict(MetricMatcher evicted) {
    for (MetricName toRemove : metricNamesForMetricMatchers.get(evicted)) {
      metricCache.invalidate(toRemove);
    }
    metricNamesForMetricMatchers.invalidate(evicted);
  }
}
