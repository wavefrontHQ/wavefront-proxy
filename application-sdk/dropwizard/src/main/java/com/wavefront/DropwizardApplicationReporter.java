package com.wavefront;

/**
 * An interface to report metrics and histograms for your DropwizardApplication
 *
 * @author Sushant Dewan (sushant@wavefront.com).
 */
public interface DropwizardApplicationReporter {
  /**
   *
   * @param metricName         Name of the Counter to be reported
   */
  void incrementCounter(String metricName);

  /**
   *
   * @param metricName         Name of the histogram to be reported
   * @param latencyMillis      API latency in millis
   */
  void updateHistogram(String metricName, long latencyMillis);
}
