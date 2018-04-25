package com.codahale.metrics;

import com.google.common.annotations.VisibleForTesting;

import com.wavefront.common.Constants;

/**
 * A counter for Wavefront delta metrics.
 *
 * Differs from a counter in that it is reset in the WavefrontReporter every time the value is reported.
 *
 * @author Vikram Raman (vikram@wavefront.com)
 */
public class DeltaCounter extends Counter {

  @VisibleForTesting
  public static synchronized DeltaCounter get(MetricRegistry registry, String metricName) {

    if (registry == null || metricName == null || metricName.isEmpty()) {
      throw new IllegalArgumentException("Invalid arguments");
    }

    if (!(metricName.startsWith(Constants.DELTA_PREFIX) || metricName.startsWith(Constants.DELTA_PREFIX_2))) {
      metricName = Constants.DELTA_PREFIX + metricName;
    }
    DeltaCounter counter = new DeltaCounter();
    registry.register(metricName, counter);
    return counter;
  }
}