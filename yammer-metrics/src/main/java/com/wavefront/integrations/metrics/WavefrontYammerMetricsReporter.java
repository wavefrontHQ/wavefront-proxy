package com.wavefront.integrations.metrics;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.AbstractPollingReporter;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class WavefrontYammerMetricsReporter extends AbstractPollingReporter {

  protected static final Logger logger = Logger.getLogger(WavefrontYammerMetricsReporter.class.getCanonicalName());
  private SocketMetricsProcessor socketMetricProcessor;
  private Function<MetricName, MetricName> transformer;

  public WavefrontYammerMetricsReporter(MetricsRegistry metricsRegistry, String name, String hostname, int port,
                                        int wavefrontHistogramPort, Supplier<Long> timeSupplier) throws IOException {
    this(metricsRegistry, name, hostname, port, wavefrontHistogramPort, timeSupplier, false, null);
  }

  /**
   * Reporter of a Yammer metrics registry to Wavefront
   * @param metricsRegistry The registry to scan-and-report
   * @param name A human readable name for this reporter
   * @param hostname The remote host where the wavefront proxy resides
   * @param port Listening port on Wavefront proxy of graphite-like telemetry data
   * @param wavefrontHistogramPort Listening port for Wavefront histogram data
   * @param timeSupplier Get current timestamp, stubbed for testing
   * @param prependGroupName If true, outgoing telemetry is of the form "group.name" rather than "name".
   * @param transformer If present, applied to each MetricName before flushing to Wavefront. This is useful for adding
   *                    point tags. Warning: this is called once per metric per scan, so it should probably be
   *                    performant.
   * @throws IOException When we can't remotely connect to Wavefront.
   */
  public WavefrontYammerMetricsReporter(MetricsRegistry metricsRegistry, String name, String hostname, int port,
                                        int wavefrontHistogramPort, Supplier<Long> timeSupplier,
                                        boolean prependGroupName,
                                        @Nullable Function<MetricName, MetricName> transformer)
      throws IOException {
    super(metricsRegistry, name);
    this.transformer = transformer;
    this.socketMetricProcessor = new SocketMetricsProcessor(hostname, port, wavefrontHistogramPort, timeSupplier,
        prependGroupName);
  }

  @Override
  public void run() {
    try {
      for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : getMetricsRegistry().groupedMetrics().entrySet()) {
        for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
          MetricName metricName = subEntry.getKey();
          if (transformer != null) {
            metricName = transformer.apply(metricName);
          }
          subEntry.getValue().processWith(socketMetricProcessor, metricName, null);
        }
      }
      socketMetricProcessor.flush();
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Cannot report point to Wavefront! Trying again next iteration.", e);
    }
  }
}
