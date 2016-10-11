package com.wavefront.agent.logsharvesting;

import com.wavefront.agent.PointHandler;

import sunnylabs.report.ReportPoint;
import sunnylabs.report.TimeSeries;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class FlushProcessorContext {
  private TimeSeries timeSeries;
  private PointHandler pointHandler;
  private String prefix;

  public FlushProcessorContext(TimeSeries timeSeries, String prefix, PointHandler pointHandler) {
    this.timeSeries = TimeSeries.newBuilder(timeSeries).build();
    this.pointHandler = pointHandler;
    this.prefix = prefix;
  }

  public TimeSeries getTimeSeries() {
    return timeSeries;
  }

  public PointHandler getPointHandler() {
    return pointHandler;
  }

  public ReportPoint.Builder reportPointBuilder() {
    return ReportPoint.newBuilder()
        .setHost(timeSeries.getHost())
        .setAnnotations(timeSeries.getAnnotations())
        .setMetric(prefix == null ? timeSeries.getMetric() : prefix + "." + timeSeries.getMetric());
  }

}
