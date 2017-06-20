package com.wavefront.agent.logsharvesting;

import com.wavefront.agent.PointHandler;

import wavefront.report.Histogram;
import wavefront.report.ReportPoint;
import wavefront.report.TimeSeries;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class FlushProcessorContext {
  private final long timestamp;
  private TimeSeries timeSeries;
  private PointHandler pointHandler;
  private String prefix;

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

  void report(ReportPoint reportPoint) {
    pointHandler.reportPoint(reportPoint, reportPoint.toString());
  }

  void report(double value) {
    report(reportPointBuilder().setValue(value).build());
  }

  void report(long value) {
    report(reportPointBuilder().setValue(value).build());
  }

  void report(Histogram value) {
    report(reportPointBuilder().setValue(value).build());
  }

  void reportSubMetric(double value, String subMetric) {
    ReportPoint.Builder builder = reportPointBuilder();
    report(builder.setValue(value).setMetric(builder.getMetric() + "." + subMetric).build());
  }

}
