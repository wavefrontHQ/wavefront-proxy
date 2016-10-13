package com.wavefront.agent.logsharvesting;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class SingleMetricRegistry extends MetricsRegistry {
  SingleMetricRegistry(Metric metric) {
    getOrAdd(new MetricName(SingleMetricRegistry.class, "singleton"), metric);
  }
}
