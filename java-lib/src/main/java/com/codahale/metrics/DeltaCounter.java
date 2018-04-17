package com.codahale.metrics;

import com.google.common.annotations.VisibleForTesting;

/**
 * A counter for Wavefront delta metrics.
 *
 * Differs from a counter in that it is reset in the WavefrontReporter every time the value is reported.
 *
 * @author Vikram Raman (vikram@wavefront.com)
 */
public class DeltaCounter extends Counter {

  public static final String DELTA_PREFIX = "\u2206"; // ∆: INCREMENT
  private static final String ALT_DELTA_PREFIX = "\u0394"; // Δ: GREEK CAPITAL LETTER DELTA

  @VisibleForTesting
  public static synchronized DeltaCounter get(MetricRegistry registry, String metricName) {

    if (registry == null || metricName == null || metricName.isEmpty()) {
      throw new IllegalArgumentException("Invalid arguments");
    }

    if (!(metricName.startsWith(DELTA_PREFIX) || metricName.startsWith(ALT_DELTA_PREFIX))) {
      metricName = DELTA_PREFIX + metricName;
    }
    DeltaCounter counter = new DeltaCounter();
    registry.register(metricName, counter);
    return counter;
  }
}