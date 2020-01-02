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

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

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
            if ((cause == RemovalCause.EXPIRED || cause == RemovalCause.EXPLICIT) &&
                metricsRegistry.allMetrics().get(key) == value) {
              metricsRegistry.removeMetric(key);
            }
          }
        })
        .build();
    this.metricNamesForMetricMatchers = Caffeine.newBuilder()
        .build((metricMatcher) -> Sets.newHashSet());
  }

  public Counter getCounter(MetricName metricName, MetricMatcher metricMatcher) {
    if (useDeltaCounters) {
      // use delta counters instead of regular counters. It helps with load balancers present in
      // front of proxy (PUB-125)
      MetricName newMetricName = DeltaCounter.getDeltaCounterMetricName(metricName);
      return put(newMetricName, metricMatcher,
          key -> DeltaCounter.get(metricsRegistry, newMetricName));
    } else {
      return put(metricName, metricMatcher, metricsRegistry::newCounter);
    }
  }

  public Gauge getGauge(MetricName metricName, MetricMatcher metricMatcher) {
    return put(metricName, metricMatcher,
        key -> metricsRegistry.newGauge(key, new ChangeableGauge<Double>()));
  }

  public Histogram getHistogram(MetricName metricName, MetricMatcher metricMatcher) {
    return put(metricName, metricMatcher, key -> wavefrontHistograms ?
        WavefrontHistogram.get(metricsRegistry, key, this.nowMillis) :
        metricsRegistry.newHistogram(metricName, false));
  }

  public synchronized void evict(MetricMatcher evicted) {
    for (MetricName toRemove : Objects.requireNonNull(metricNamesForMetricMatchers.get(evicted))) {
      metricCache.invalidate(toRemove);
    }
    metricNamesForMetricMatchers.invalidate(evicted);
  }

  public void cleanUp() {
    metricCache.cleanUp();
  }

  @SuppressWarnings("unchecked")
  private <M extends Metric> M put(MetricName metricName, MetricMatcher metricMatcher,
                                   Function<MetricName, M> getter) {
    @Nullable
    Metric cached = metricCache.getIfPresent(metricName);
    Objects.requireNonNull(metricNamesForMetricMatchers.get(metricMatcher)).add(metricName);
    if (cached != null && cached == metricsRegistry.allMetrics().get(metricName)) {
      return (M) cached;
    }
    return (M) metricCache.asMap().compute(metricName, (name, existing) -> {
      @Nullable
      Metric expected = metricsRegistry.allMetrics().get(name);
      return expected == null ? getter.apply(name) : expected;
    });
  }
}
