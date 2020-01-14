package com.wavefront.agent.logsharvesting;

import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.TDigest;
import com.wavefront.common.MetricsToTimeseries;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.DeltaCounter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.WavefrontHistogram;
import com.yammer.metrics.stats.Snapshot;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import wavefront.report.HistogramType;

/**
 * Wrapper for {@link com.yammer.metrics.core.MetricProcessor}. It provides additional support
 * for Delta Counters and WavefrontHistogram.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class FlushProcessor implements MetricProcessor<FlushProcessorContext> {

  private final Counter sentCounter = Metrics.newCounter(new MetricName("logsharvesting", "", "sent"));
  private final Counter histogramCounter = Metrics.newCounter(new MetricName("logsharvesting", "", "histograms-sent"));
  private final Supplier<Long> currentMillis;
  private final boolean useWavefrontHistograms;
  private final boolean reportEmptyHistogramStats;

  /**
   * Create new FlushProcessor instance
   *
   * @param currentMillis             {@link Supplier} of time (in milliseconds)
   * @param useWavefrontHistograms    export data in {@link com.yammer.metrics.core.WavefrontHistogram} format
   * @param reportEmptyHistogramStats enable legacy {@link com.yammer.metrics.core.Histogram} behavior and send zero
   *                                  values for every stat
   */
  FlushProcessor(Supplier<Long> currentMillis, boolean useWavefrontHistograms,
                 boolean reportEmptyHistogramStats) {
    this.currentMillis = currentMillis;
    this.useWavefrontHistograms = useWavefrontHistograms;
    this.reportEmptyHistogramStats = reportEmptyHistogramStats;
  }

  @Override
  public void processMeter(MetricName name, Metered meter, FlushProcessorContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void processCounter(MetricName name, Counter counter, FlushProcessorContext context) {
    long count;
    // handle delta counter
    if (counter instanceof DeltaCounter) {
      count = DeltaCounter.processDeltaCounter((DeltaCounter) counter);
      if (count == 0) return; // do not report 0-value delta counters
    } else {
      count = counter.count();
    }
    context.report(count);
    sentCounter.inc();
  }

  @Override
  public void processHistogram(MetricName name, Histogram histogram, FlushProcessorContext context) {
    if (histogram instanceof WavefrontHistogram) {
      WavefrontHistogram wavefrontHistogram = (WavefrontHistogram) histogram;
      if (useWavefrontHistograms) {
        // export Wavefront histograms in its native format
        if (wavefrontHistogram.count() == 0) return;
        for (WavefrontHistogram.MinuteBin bin : wavefrontHistogram.bins(true)) {
          if (bin.getDist().size() == 0) continue;
          int size = bin.getDist().centroids().size();
          List<Double> centroids = new ArrayList<>(size);
          List<Integer> counts = new ArrayList<>(size);
          for (Centroid centroid : bin.getDist().centroids()) {
            centroids.add(centroid.mean());
            counts.add(centroid.count());
          }
          context.report(wavefront.report.Histogram.newBuilder().
              setDuration(60_000). // minute bins
              setType(HistogramType.TDIGEST).
              setBins(centroids).
              setCounts(counts).
              build(), bin.getMinMillis());
          histogramCounter.inc();
        }
      } else {
        // convert Wavefront histogram to Yammer-style histogram
        TDigest tDigest = new AVLTreeDigest(100);
        List<WavefrontHistogram.MinuteBin> bins = wavefrontHistogram.bins(true);
        bins.stream().map(WavefrontHistogram.MinuteBin::getDist).forEach(tDigest::add);
        context.reportSubMetric(tDigest.centroids().stream().mapToLong(Centroid::count).sum(), "count");
        Summarizable summarizable = new Summarizable() {
          @Override
          public double max() {
            return tDigest.centroids().stream().map(Centroid::mean).max(Comparator.naturalOrder()).orElse(Double.NaN);
          }

          @Override
          public double min() {
            return tDigest.centroids().stream().map(Centroid::mean).min(Comparator.naturalOrder()).orElse(Double.NaN);
          }

          @Override
          public double mean() {
            Centroid mean = tDigest.centroids().stream().
                reduce((x, y) -> new Centroid(x.mean() + (y.mean() * y.count()), x.count() + y.count())).orElse(null);
            return mean == null || tDigest.centroids().size() == 0 ? Double.NaN : mean.mean() / mean.count();
          }

          @Override
          public double stdDev() {
            return Double.NaN;
          }

          @Override
          public double sum() {
            return Double.NaN;
          }
        };
        for (Map.Entry<String, Double> entry : MetricsToTimeseries.explodeSummarizable(summarizable,
            reportEmptyHistogramStats).entrySet()) {
          if (!entry.getValue().isNaN()) {
            context.reportSubMetric(entry.getValue(), entry.getKey());
          }
        }
        Sampling sampling = () -> new Snapshot(new double[0]) {
          @Override
          public double get75thPercentile() {
            return tDigest.quantile(.75);
          }

          @Override
          public double get95thPercentile() {
            return tDigest.quantile(.95);
          }

          @Override
          public double get98thPercentile() {
            return tDigest.quantile(.98);
          }

          @Override
          public double get999thPercentile() {
            return tDigest.quantile(.999);
          }

          @Override
          public double get99thPercentile() {
            return tDigest.quantile(.99);
          }

          @Override
          public double getMedian() {
            return tDigest.quantile(.50);
          }

          @Override
          public double getValue(double quantile) {
            return tDigest.quantile(quantile);
          }

          @Override
          public double[] getValues() {
            return new double[0];
          }

          @Override
          public int size() {
            return (int) tDigest.size();
          }
        };
        for (Map.Entry<String, Double> entry : MetricsToTimeseries.explodeSampling(sampling,
            reportEmptyHistogramStats).entrySet()) {
          if (!entry.getValue().isNaN()) {
            context.reportSubMetric(entry.getValue(), entry.getKey());
          }
        }
        sentCounter.inc();
      }
    } else {
      context.reportSubMetric(histogram.count(), "count");
      for (Map.Entry<String, Double> entry : MetricsToTimeseries.explodeSummarizable(histogram, reportEmptyHistogramStats).entrySet()) {
        if (!entry.getValue().isNaN()) {
          context.reportSubMetric(entry.getValue(), entry.getKey());
        }
      }
      for (Map.Entry<String, Double> entry : MetricsToTimeseries.explodeSampling(histogram, reportEmptyHistogramStats).entrySet()) {
        if (!entry.getValue().isNaN()) {
          context.reportSubMetric(entry.getValue(), entry.getKey());
        }
      }
      sentCounter.inc();
      histogram.clear();
    }
  }

  @Override
  public void processTimer(MetricName name, Timer timer, FlushProcessorContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void processGauge(MetricName name, Gauge<?> gauge, FlushProcessorContext context) {
    @SuppressWarnings("unchecked")
    ChangeableGauge<Double> changeableGauge = (ChangeableGauge<Double>) gauge;
    Double value = changeableGauge.value();
    if (value == null || value.isInfinite() || value.isNaN()) return;
    context.report(value);
    sentCounter.inc();
  }
}
