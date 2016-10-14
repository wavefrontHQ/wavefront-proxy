package com.wavefront.agent.logsharvesting;


import com.github.benmanes.caffeine.cache.CacheLoader;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.WavefrontHistogram;

import java.util.function.Supplier;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class MetricCacheLoader implements CacheLoader<MetricName, Metric> {
  private MetricsRegistry metricsRegistry;
  private Supplier<Long> currentMillis;

  MetricCacheLoader(MetricsRegistry metricsRegistry, Supplier<Long> currentMillis) {
    this.metricsRegistry = metricsRegistry;
    this.currentMillis = currentMillis;
  }

  @Override
  public Metric load(MetricName key) throws Exception {
    if (key.getType().equals(Counter.class.getTypeName())) {
      return metricsRegistry.newCounter(key);
    } else if (key.getType().equals(Histogram.class.getTypeName())) {
      return metricsRegistry.newHistogram(key, false);
    } else if (key.getType().equals(Gauge.class.getTypeName())) {
      return metricsRegistry.newGauge(key, new ChangeableGauge<Double>());
    } else if (key.getType().equals(WavefrontHistogram.class.getTypeName())) {
      return WavefrontHistogram.get(metricsRegistry, key, this.currentMillis);
    }
    throw new RuntimeException("Unsupported metric type: " + key.getType());
  }
}
