package com.wavefront.agent;

import com.yammer.metrics.core.MetricsRegistry;

public class SharedMetricsRegistry extends MetricsRegistry {

  private static final SharedMetricsRegistry INSTANCE = new SharedMetricsRegistry();

  private SharedMetricsRegistry() {
  }

  public static SharedMetricsRegistry getInstance() {
    return INSTANCE;
  }
}
