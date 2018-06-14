package com.wavefront.metrics;

import com.yammer.metrics.core.MetricName;

/**
 * There are some metrics that need to have well known names.
 *
 * @author Andrew Kao (andrew@wavefront.com)
 */
public enum ExpectedAgentMetric {
  ACTIVE_LISTENERS(new MetricName("listeners", "", "active")),
  BUFFER_BYTES_LEFT(new MetricName("buffer", "", "bytes-left")),
  BUFFER_BYTES_PER_MINUTE(new MetricName("buffer", "", "fill-rate")),
  CURRENT_QUEUE_SIZE(new MetricName("buffer", "", "task-count")),
  RDNS_CACHE_SIZE(new MetricName("listeners", "", "rdns-cache-size"));

  public MetricName metricName;

  public String getCombinedName() {
    return metricName.getGroup() + "." + metricName.getName();
  }

  ExpectedAgentMetric(MetricName metricName) {
    this.metricName = metricName;
  }
}
