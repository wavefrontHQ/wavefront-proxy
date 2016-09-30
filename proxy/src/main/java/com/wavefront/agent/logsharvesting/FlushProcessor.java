package com.wavefront.agent.logsharvesting;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.Timer;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

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
    throw new NotImplementedException();
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
