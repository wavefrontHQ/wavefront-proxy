package com.wavefront.agent.logsharvesting;

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.databind.JsonNode;
import com.wavefront.metrics.JsonMetricsGenerator;
import com.wavefront.metrics.JsonMetricsParser;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;

import sunnylabs.report.ReportPoint;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class FlushProcessor implements MetricProcessor<FlushProcessorContext> {

  @Override
  public void processMeter(MetricName name, Metered meter, FlushProcessorContext context) throws Exception {
    throw new NotImplementedException();
  }

  @Override
  public void processCounter(MetricName name, Counter counter, FlushProcessorContext context) throws Exception {
    context.getPointHandler().reportPoint(
        context.reportPointBuilder()
            .setValue(counter.count())
            .setTimestamp(System.currentTimeMillis()).build(),
        null);
  }

  @Override
  public void processHistogram(MetricName name, Histogram histogram, FlushProcessorContext context) throws Exception {
    // HACK. Convert histogram to/from JSON using wavefront libraries to explode the yammer histogram
    // into canonical wavefront gauges, e.g. foo.count, foo.min, foo.max, ...
    List<ReportPoint> reportPoints = Lists.newLinkedList();
    JsonNode jsonNode = JsonMetricsGenerator.generateJsonMetrics(new SingleMetricRegistry(histogram),
        false, false, false);
    JsonMetricsParser.report(
        context.getTimeSeries().getTable(), "", jsonNode, reportPoints, context.getTimeSeries().getHost(),
        System.currentTimeMillis());
    // We now have a list of points like "foo.*.count, foo.*.min, ..."
    for (ReportPoint template : reportPoints) {
      int idx = template.getMetric().lastIndexOf('.');
      Double value = template.getValue() instanceof Long
          ? (Long) template.getValue() * 1.0
          : (Double) template.getValue();
      String baseName = template.getMetric().substring(idx);
      ReportPoint reportPoint = context.reportPointBuilder()
          .setValue(value)
          .setMetric(context.getMetricName() + baseName)
          .setTimestamp(System.currentTimeMillis()).build();
      context.getPointHandler().reportPoint(reportPoint, null);
    }
  }

  @Override
  public void processTimer(MetricName name, Timer timer, FlushProcessorContext context) throws Exception {
    throw new NotImplementedException();
  }

  @Override
  public void processGauge(MetricName name, Gauge<?> gauge, FlushProcessorContext context) throws Exception {
    ChangeableGauge<Double> changeableGauge = (ChangeableGauge<Double>) gauge;
    context.getPointHandler().reportPoint(
        context.reportPointBuilder()
            .setValue(changeableGauge.value())
            .setTimestamp(System.currentTimeMillis()).build(),
        null);

  }
}
