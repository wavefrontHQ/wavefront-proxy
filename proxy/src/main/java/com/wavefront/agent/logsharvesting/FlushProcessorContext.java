package com.wavefront.agent.logsharvesting;

import com.wavefront.agent.PointHandler;

import sunnylabs.report.Histogram;
import sunnylabs.report.ReportPoint;
import sunnylabs.report.TimeSeries;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class FlushProcessorContext {
  private TimeSeries timeSeries;
  private PointHandler pointHandler;
  private String prefix;
  private final long timestamp;

  FlushProcessorContext(TimeSeries timeSeries, String prefix, PointHandler pointHandler) {
    this.timeSeries = TimeSeries.newBuilder(timeSeries).build();
    this.pointHandler = pointHandler;
    this.prefix = prefix;
    timestamp = System.currentTimeMillis();
  }

  String getMetricName() {
    return timeSeries.getMetric();
  }

  private ReportPoint.Builder reportPointBuilder() {
    return ReportPoint.newBuilder()
        .setHost(timeSeries.getHost())
        .setAnnotations(timeSeries.getAnnotations())
        .setTimestamp(timestamp)
        .setMetric(prefix == null ? timeSeries.getMetric() : prefix + "." + timeSeries.getMetric());
  }

  void report(double value) {
    pointHandler.reportPoint(reportPointBuilder().setValue(value).build(), null);
  }

  void report(long value) {
    pointHandler.reportPoint(reportPointBuilder().setValue(value).build(), null);
  }

  void report(Histogram value) {
    pointHandler.reportPoint(reportPointBuilder().setValue(value).build(), null);
  }

  void reportSubMetric(double value, String subMetric) {
    ReportPoint.Builder builder = reportPointBuilder();
    pointHandler.reportPoint(
        builder.setValue(value).setMetric(builder.getMetric() + "." + subMetric).build(), null);
  }

}
