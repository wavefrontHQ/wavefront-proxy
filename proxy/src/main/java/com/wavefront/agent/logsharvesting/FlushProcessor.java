package com.wavefront.agent.logsharvesting;

import com.beust.jcommander.internal.Lists;
import com.wavefront.common.MetricsToTimeseries;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.WavefrontHistogram;

import java.util.Map;
import java.util.function.Supplier;

import wavefront.report.HistogramType;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
class FlushProcessor implements MetricProcessor<FlushProcessorContext> {

  private final Counter sentCounter;
  private final Supplier<Long> currentMillis;

  FlushProcessor(Counter sentCounter, Supplier<Long> currentMillis) {
    this.sentCounter = sentCounter;
    this.currentMillis = currentMillis;
  }

  @Override
  public void processMeter(MetricName name, Metered meter, FlushProcessorContext context) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void processCounter(MetricName name, Counter counter, FlushProcessorContext context) throws Exception {
    context.report(counter.count());
    sentCounter.inc();
  }

  @Override
  public void processHistogram(MetricName name, Histogram histogram, FlushProcessorContext context) throws Exception {
    if (histogram instanceof WavefrontHistogram) {
      WavefrontHistogram wavefrontHistogram = (WavefrontHistogram) histogram;
      wavefront.report.Histogram.Builder builder = wavefront.report.Histogram.newBuilder();
      builder.setBins(Lists.newLinkedList());
      builder.setCounts(Lists.newLinkedList());
      long minMillis = Long.MAX_VALUE;
      if (wavefrontHistogram.count() == 0) return;
      for (WavefrontHistogram.MinuteBin minuteBin : wavefrontHistogram.bins(true)) {
        builder.getBins().add(minuteBin.getDist().quantile(.5));
        builder.getCounts().add(Math.toIntExact(minuteBin.getDist().size()));
        minMillis = Long.min(minMillis, minuteBin.getMinMillis());
      }
      builder.setType(HistogramType.TDIGEST);
      builder.setDuration(Math.toIntExact(currentMillis.get() - minMillis));
      context.report(builder.build());
    } else {
      context.reportSubMetric(histogram.count(), "count");
      for (Map.Entry<String, Double> entry : MetricsToTimeseries.explodeSummarizable(histogram).entrySet()) {
        context.reportSubMetric(entry.getValue(), entry.getKey());
      }
      for (Map.Entry<String, Double> entry : MetricsToTimeseries.explodeSampling(histogram).entrySet()) {
        context.reportSubMetric(entry.getValue(), entry.getKey());
      }
      histogram.clear();
    }
    sentCounter.inc();
  }

  @Override
  public void processTimer(MetricName name, Timer timer, FlushProcessorContext context) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void processGauge(MetricName name, Gauge<?> gauge, FlushProcessorContext context) throws Exception {
    @SuppressWarnings("unchecked")
    ChangeableGauge<Double> changeableGauge = (ChangeableGauge<Double>) gauge;
    Double value = changeableGauge.value();
    if (value == null || value.isInfinite() || value.isNaN()) return;
    context.report(value);
    sentCounter.inc();
  }
}
