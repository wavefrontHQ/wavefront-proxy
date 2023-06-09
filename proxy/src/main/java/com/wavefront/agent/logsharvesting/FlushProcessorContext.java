package com.wavefront.agent.logsharvesting;

import com.wavefront.agent.core.handlers.ReportableEntityHandler;
import com.wavefront.common.MetricConstants;
import java.util.function.Supplier;
import wavefront.report.Histogram;
import wavefront.report.ReportPoint;
import wavefront.report.TimeSeries;

public class FlushProcessorContext {
  private final long timestamp;
  private final TimeSeries timeSeries;
  private final Supplier<ReportableEntityHandler<ReportPoint>> pointHandlerSupplier;
  private final Supplier<ReportableEntityHandler<ReportPoint>> histogramHandlerSupplier;
  private final String prefix;

  FlushProcessorContext(
      TimeSeries timeSeries,
      String prefix,
      Supplier<ReportableEntityHandler<ReportPoint>> pointHandlerSupplier,
      Supplier<ReportableEntityHandler<ReportPoint>> histogramHandlerSupplier) {
    this.timeSeries = TimeSeries.newBuilder(timeSeries).build();
    this.prefix = prefix;
    this.pointHandlerSupplier = pointHandlerSupplier;
    this.histogramHandlerSupplier = histogramHandlerSupplier;
    timestamp = System.currentTimeMillis();
  }

  private ReportPoint.Builder reportPointBuilder(long timestamp) {
    String newName = timeSeries.getMetric();
    // if prefix is provided then add the delta before the prefix
    if (prefix != null
        && (newName.startsWith(MetricConstants.DELTA_PREFIX)
            || newName.startsWith(MetricConstants.DELTA_PREFIX_2))) {
      newName =
          MetricConstants.DELTA_PREFIX
              + prefix
              + "."
              + newName.substring(MetricConstants.DELTA_PREFIX.length());
    } else {
      newName = prefix == null ? timeSeries.getMetric() : prefix + "." + timeSeries.getMetric();
    }
    return ReportPoint.newBuilder()
        .setHost(timeSeries.getHost())
        .setAnnotations(timeSeries.getAnnotations())
        .setTimestamp(timestamp)
        .setMetric(newName);
  }

  void report(ReportPoint reportPoint) {
    pointHandlerSupplier.get().report(reportPoint);
  }

  void report(double value) {
    report(reportPointBuilder(this.timestamp).setValue(value).build());
  }

  void report(long value) {
    report(reportPointBuilder(this.timestamp).setValue(value).build());
  }

  void report(Histogram value, long timestamp) {
    histogramHandlerSupplier.get().report(reportPointBuilder(timestamp).setValue(value).build());
  }

  void reportSubMetric(double value, String subMetric) {
    ReportPoint.Builder builder = reportPointBuilder(this.timestamp);
    report(builder.setValue(value).setMetric(builder.getMetric() + "." + subMetric).build());
  }
}
