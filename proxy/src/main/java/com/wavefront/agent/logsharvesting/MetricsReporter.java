package com.wavefront.agent.logsharvesting;

import com.wavefront.agent.PointHandler;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.AbstractPollingReporter;

import java.util.Map;
import java.util.SortedMap;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import sunnylabs.report.TimeSeries;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class MetricsReporter extends AbstractPollingReporter {

  protected static final Logger logger = Logger.getLogger(MetricsReporter.class.getCanonicalName());
  private final FlushProcessor flushProcessor;
  private final PointHandler pointHandler;
  private final String prefix;

  public MetricsReporter(MetricsRegistry metricsRegistry, FlushProcessor flushProcessor, String name,
                         PointHandler pointHandler, String prefix) {
    super(metricsRegistry, name);
    this.flushProcessor = flushProcessor;
    this.pointHandler = pointHandler;
    this.prefix = prefix;
  }

  @Override
  public void run() {
    for (Map.Entry<String, SortedMap<MetricName, Metric>> group : getMetricsRegistry().groupedMetrics().entrySet()) {
      for (Map.Entry<MetricName, Metric> entry : group.getValue().entrySet()) {
        if (entry.getValue() == null || entry.getKey() == null) {
          logger.severe("Application Error! Pulled null value from metrics registry.");
        }
        MetricName metricName = entry.getKey();
        Metric metric = entry.getValue();
        try {
          TimeSeries timeSeries = TimeSeriesUtils.fromMetricName(metricName);
          metric.processWith(flushProcessor, metricName, new FlushProcessorContext(timeSeries, prefix, pointHandler));
        } catch (Exception e) {
          logger.log(Level.SEVERE, "Uncaught exception in MetricsReporter", e);
        }
      }
    }
  }

}
