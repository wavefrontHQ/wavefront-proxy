package com.wavefront.integrations.metrics;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.AbstractPollingReporter;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class WavefrontYammerMetricsReporter extends AbstractPollingReporter {

  protected static final Logger logger = Logger.getLogger(WavefrontYammerMetricsReporter.class.getCanonicalName());
  private SocketMetricsProcessor socketMetricProcessor;

  public WavefrontYammerMetricsReporter(MetricsRegistry metricsRegistry, String name, String hostname, int port,
                                        int wavefrontHistogramPort, Supplier<Long> timeSupplier)
      throws IOException {
    super(metricsRegistry, name);
    this.socketMetricProcessor = new SocketMetricsProcessor(hostname, port, wavefrontHistogramPort, timeSupplier);
  }

  @Override
  public void run() {
    try {
      for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : getMetricsRegistry().groupedMetrics().entrySet()) {
        for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
          subEntry.getValue().processWith(socketMetricProcessor, subEntry.getKey(), null);
        }
      }
      socketMetricProcessor.flush();
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Cannot report point to Wavefront! Trying again next iteration.", e);
    }
  }
}
