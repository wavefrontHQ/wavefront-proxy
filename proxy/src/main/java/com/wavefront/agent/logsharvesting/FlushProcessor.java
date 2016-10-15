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
import com.yammer.metrics.core.WavefrontHistogram;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;
import java.util.function.Supplier;

import sunnylabs.report.HistogramType;
import sunnylabs.report.ReportPoint;

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
    throw new NotImplementedException();
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
      sunnylabs.report.Histogram.Builder builder = sunnylabs.report.Histogram.newBuilder();
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
      context.reportSubMetric(histogram.min(), "min");
      context.reportSubMetric(histogram.max(), "max");
      context.reportSubMetric(histogram.mean(), "mean");
      context.reportSubMetric(histogram.getSnapshot().getMedian(), "median");
      context.reportSubMetric(histogram.getSnapshot().get75thPercentile(), "p75");
      context.reportSubMetric(histogram.getSnapshot().get95thPercentile(), "p95");
      context.reportSubMetric(histogram.getSnapshot().get99thPercentile(), "p99");
      context.reportSubMetric(histogram.getSnapshot().get999thPercentile(), "p999");
      histogram.clear();
    }
    sentCounter.inc();
  }

  @Override
  public void processTimer(MetricName name, Timer timer, FlushProcessorContext context) throws Exception {
    throw new NotImplementedException();
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
