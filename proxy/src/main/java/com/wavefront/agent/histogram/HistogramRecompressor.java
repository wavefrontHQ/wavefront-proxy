package com.wavefront.agent.histogram;

import com.google.common.annotations.VisibleForTesting;
import com.tdunning.math.stats.AgentDigest;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.common.Utils;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import wavefront.report.Histogram;
import wavefront.report.HistogramType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.wavefront.agent.histogram.HistogramUtils.mergeHistogram;

/**
 * Recompresses histograms to reduce their size.
 *
 * @author vasily@wavefront.com
 */
public class HistogramRecompressor implements Function<Histogram, Histogram> {
  private final Supplier<Short> storageAccuracySupplier;
  private final Supplier<Counter> histogramsCompacted = Utils.lazySupplier(() ->
      Metrics.newCounter(new TaggedMetricName("histogram", "histograms_compacted")));
  private final Supplier<Counter> histogramsRecompressed = Utils.lazySupplier(() ->
      Metrics.newCounter(new TaggedMetricName("histogram", "histograms_recompressed")));

  /**
   * @param storageAccuracySupplier Supplier for histogram storage accuracy
   */
  public HistogramRecompressor(Supplier<Short> storageAccuracySupplier) {
    this.storageAccuracySupplier = storageAccuracySupplier;
  }

  @Override
  public Histogram apply(Histogram input) {
    Histogram result = input;
    if (hasDuplicateCentroids(input)) {
      // merge centroids with identical values first, and if we get the number of centroids
      // low enough, we might not need to incur recompression overhead after all.
      result = compactCentroids(input);
      histogramsCompacted.get().inc();
    }
    if (result.getBins().size() > 2 * storageAccuracySupplier.get()) {
      AgentDigest digest = new AgentDigest(storageAccuracySupplier.get(), 0);
      mergeHistogram(digest, result);
      digest.compress();
      result = digest.toHistogram(input.getDuration());
      histogramsRecompressed.get().inc();
    }
    return result;
  }

  @VisibleForTesting
  static boolean hasDuplicateCentroids(wavefront.report.Histogram histogram) {
    Set<Double> uniqueBins = new HashSet<>();
    for (Double bin : histogram.getBins()) {
      if (!uniqueBins.add(bin)) return true;
    }
    return false;
  }

  @VisibleForTesting
  static wavefront.report.Histogram compactCentroids(wavefront.report.Histogram histogram) {
    List<Double> bins = histogram.getBins();
    List<Integer> counts = histogram.getCounts();
    int numCentroids = Math.min(bins.size(), counts.size());

    List<Double> newBins = new ArrayList<>();
    List<Integer> newCounts = new ArrayList<>();

    Double accumulatedValue = null;
    int accumulatedCount = 0;
    for (int i = 0; i < numCentroids; ++i) {
      double value = bins.get(i);
      int count = counts.get(i);
      if (accumulatedValue == null) {
        accumulatedValue = value;
      } else if (value != accumulatedValue) {
        newBins.add(accumulatedValue);
        newCounts.add(accumulatedCount);
        accumulatedValue = value;
        accumulatedCount = 0;
      }
      accumulatedCount += count;
    }
    if (accumulatedValue != null) {
      newCounts.add(accumulatedCount);
      newBins.add(accumulatedValue);
    }
    return wavefront.report.Histogram.newBuilder()
        .setDuration(histogram.getDuration())
        .setBins(newBins)
        .setCounts(newCounts)
        .setType(HistogramType.TDIGEST)
        .build();
  }
}
