package com.yammer.metrics.core;

import com.google.common.annotations.VisibleForTesting;

import com.wavefront.common.MetricConstants;
import com.yammer.metrics.Metrics;

/**
 * A counter for Wavefront delta metrics.
 *
 * Differs from a counter in that it is reset every time the value is reported.
 *
 * (This is similar to how {@link com.yammer.metrics.core.WavefrontHistogram} is implemented)
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com)
 */
public class DeltaCounter extends Counter {

  private DeltaCounter() {
    // nothing to do. keeping it private so that it can't be instantiated directly
  }

  /**
   * A static factory method to create an instance of Delta Counter.
   *
   * @param registry    The MetricRegistry to use
   * @param metricName  The MetricName to use
   * @return            An instance of DeltaCounter
   */
  @VisibleForTesting
  public static synchronized DeltaCounter get(MetricsRegistry registry, MetricName metricName) {
    if (registry == null || metricName == null || metricName.getName().isEmpty()) {
      throw new IllegalArgumentException("Invalid arguments");
    }

    DeltaCounter counter = new DeltaCounter();
    MetricName newMetricName = getDeltaCounterMetricName(metricName);
    registry.getOrAdd(newMetricName, counter);
    return counter;
  }

  /**
   * A static factory method to create an instance of DeltaCounter. It uses the default
   * MetricsRegistry.
   *
   * @param metricName  The MetricName to use
   * @return            An instance of DeltaCounter
   */
  public static DeltaCounter get(MetricName metricName) {
    return get(Metrics.defaultRegistry(), metricName);
  }

  /**
   * This method returns the current count of the DeltaCounter and resets the counter.
   *
   * @param counter The DeltaCounter whose value is requested
   * @return        The current count of the DeltaCounter
   */
  public static long processDeltaCounter(DeltaCounter counter) {
    long count = counter.count();
    counter.dec(count);
    return count;
  }

  /**
   * This method transforms the MetricName into a new MetricName that represents a DeltaCounter.
   * The transformation includes prepending a "\u2206" character to the name.
   *
   * @param metricName  The MetricName which needs to be transformed
   * @return            The new MetricName representing a DeltaCounter
   */
  public static MetricName getDeltaCounterMetricName(MetricName metricName) {
    if (isDelta(metricName.getName())) {
      return metricName;
    } else {
      String name = getDeltaCounterName(metricName.getName());
      return new MetricName(metricName.getGroup(), metricName.getType(), name,
          metricName.getScope());
    }
  }

  /**
   * A helper function to transform a counter name to a DeltaCounter name. The transformation
   * includes prepending a "\u2206" character to the name.
   *
   * @param name  The name which needs to be transformed
   * @return      The new name representing a DeltaCounter
   */
  public static String getDeltaCounterName(String name) {
    if (!isDelta(name)) {
      return MetricConstants.DELTA_PREFIX + name;
    } else {
      return name;
    }
  }

  /**
   * This method checks whether the name is a valid DeltaCounter name.
   *
   * @param name The name which needs to be checked
   * @return     True if the name is a valid DeltaCounter name, otherwise returns false
   */
  public static boolean isDelta(String name) {
    return name.startsWith(MetricConstants.DELTA_PREFIX) ||
        name.startsWith(MetricConstants.DELTA_PREFIX_2);
  }

  /**
   * A helper function to transform the name from a DeltaCounter name to a new name by removing
   * the "\u2206" prefix. If the name is not a DeltaCounter name then the input name is returned.
   *
   * @param name  The name which needs to be transformed
   * @return      The transformed name
   */
  public static String getNameWithoutDeltaPrefix(String name) {
    if (name.startsWith(MetricConstants.DELTA_PREFIX)) {
      return name.substring(MetricConstants.DELTA_PREFIX.length());
    } else if (name.startsWith(MetricConstants.DELTA_PREFIX_2)) {
      return name.substring(MetricConstants.DELTA_PREFIX_2.length());
    } else {
      return name;
    }
  }
}
