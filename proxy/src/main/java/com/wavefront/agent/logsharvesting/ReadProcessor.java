package com.wavefront.agent.logsharvesting;

import com.yammer.metrics.core.*;

public class ReadProcessor implements MetricProcessor<ReadProcessorContext> {
  @Override
  public void processMeter(MetricName name, Metered meter, ReadProcessorContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void processCounter(MetricName name, Counter counter, ReadProcessorContext context) {
    counter.inc(context.getValue() == null ? 1L : Math.round(context.getValue()));
  }

  @Override
  public void processHistogram(MetricName name, Histogram histogram, ReadProcessorContext context) {
    if (histogram instanceof WavefrontHistogram) {
      ((WavefrontHistogram) histogram).update(context.getValue());
    } else {
      histogram.update(Math.round(context.getValue()));
    }
  }

  @Override
  public void processTimer(MetricName name, Timer timer, ReadProcessorContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void processGauge(MetricName name, Gauge<?> gauge, ReadProcessorContext context)
      throws Exception {
    if (context.getValue() == null) {
      throw new MalformedMessageException("Need an explicit value for updating a gauge.");
    }
    ((ChangeableGauge<Double>) gauge).setValue(context.getValue());
  }
}
