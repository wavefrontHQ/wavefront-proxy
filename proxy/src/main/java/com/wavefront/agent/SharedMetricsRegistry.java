package com.wavefront.agent;

import com.yammer.metrics.core.MetricsRegistry;

public class SharedMetricsRegistry extends MetricsRegistry {

  private static SharedMetricsRegistry INSTANCE = new SharedMetricsRegistry();

  public static SharedMetricsRegistry getInstance() {
    return INSTANCE;
  }
}
