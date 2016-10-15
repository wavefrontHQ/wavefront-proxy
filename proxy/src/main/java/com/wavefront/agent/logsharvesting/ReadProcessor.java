package com.wavefront.agent.logsharvesting;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.Timer;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class ReadProcessor implements MetricProcessor<ReadProcessorContext> {
  @Override
  public void processMeter(MetricName name, Metered meter, ReadProcessorContext context) throws Exception {
    throw new NotImplementedException();
  }

  @Override
  public void processCounter(MetricName name, Counter counter, ReadProcessorContext context) throws Exception {
    counter.inc(context.getValue() == null ? 1L : Math.round(context.getValue()));
  }

  @Override
  public void processHistogram(MetricName name, Histogram histogram, ReadProcessorContext context) throws Exception {
    histogram.update(Math.round(context.getValue()));
  }

  @Override
  public void processTimer(MetricName name, Timer timer, ReadProcessorContext context) throws Exception {
    throw new NotImplementedException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void processGauge(MetricName name, Gauge<?> gauge, ReadProcessorContext context) throws Exception {
    if (context.getValue() == null) {
      throw new MalformedMessageException("Need an explicit value for updating a gauge.");
    }
    ((ChangeableGauge<Double>) gauge).setValue(context.getValue());
  }
}
