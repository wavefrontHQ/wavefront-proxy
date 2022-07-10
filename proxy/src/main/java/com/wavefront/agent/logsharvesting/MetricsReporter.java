package com.wavefront.agent.logsharvesting;

import static com.wavefront.agent.ProxyContext.queuesManager;
import static com.wavefront.common.Utils.lazySupplier;

import com.wavefront.agent.core.handlers.ReportableEntityHandler;
import com.wavefront.agent.core.handlers.ReportableEntityHandlerFactory;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import wavefront.report.ReportPoint;
import wavefront.report.TimeSeries;

public class MetricsReporter extends AbstractPollingReporter {

  protected static final Logger logger = Logger.getLogger(MetricsReporter.class.getCanonicalName());
  private final FlushProcessor flushProcessor;
  private final Supplier<ReportableEntityHandler<ReportPoint>> pointHandlerSupplier;
  private final Supplier<ReportableEntityHandler<ReportPoint>> histogramHandlerSupplier;
  private final String prefix;

  public MetricsReporter(
      MetricsRegistry metricsRegistry,
      FlushProcessor flushProcessor,
      String name,
      ReportableEntityHandlerFactory handlerFactory,
      String prefix) {
    super(metricsRegistry, name);
    this.flushProcessor = flushProcessor;
    this.pointHandlerSupplier =
        lazySupplier(
            () ->
                handlerFactory.getHandler(
                    "logs-ingester", queuesManager.initQueue(ReportableEntityType.POINT)));
    this.histogramHandlerSupplier =
        lazySupplier(
            () ->
                handlerFactory.getHandler(
                    "logs-ingester", queuesManager.initQueue(ReportableEntityType.HISTOGRAM)));
    this.prefix = prefix;
  }

  @Override
  public void run() {
    for (Map.Entry<String, SortedMap<MetricName, Metric>> group :
        getMetricsRegistry().groupedMetrics().entrySet()) {
      for (Map.Entry<MetricName, Metric> entry : group.getValue().entrySet()) {
        if (entry.getValue() == null || entry.getKey() == null) {
          logger.severe("Application Error! Pulled null value from metrics registry.");
        }
        MetricName metricName = entry.getKey();
        Metric metric = entry.getValue();
        try {
          TimeSeries timeSeries = TimeSeriesUtils.fromMetricName(metricName);
          metric.processWith(
              flushProcessor,
              metricName,
              new FlushProcessorContext(
                  timeSeries, prefix, pointHandlerSupplier, histogramHandlerSupplier));
        } catch (Exception e) {
          logger.log(Level.SEVERE, "Uncaught exception in MetricsReporter", e);
        }
      }
    }
  }
}
