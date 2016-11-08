package com.yammer.metrics.core;

import com.google.common.annotations.VisibleForTesting;

import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.TDigest;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.stats.Sample;
import com.yammer.metrics.stats.Snapshot;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Iterables.getLast;
import static java.lang.Double.MAX_VALUE;
import static java.lang.Double.MIN_VALUE;
import static java.lang.Double.NaN;

/**
 * Wavefront implementation of {@link Histogram}.
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class WavefrontHistogram extends Histogram implements Metric {
  private final static int ACCURACY = 100;
  private final static int MAX_BINS = 10;

  private final Supplier<Long> millis;
  private final LinkedList<MinuteBin> bins;


  public static class MinuteBin {
    private final TDigest dist;
    private final long minMillis;

    MinuteBin(long minMillis) {
      dist = new AVLTreeDigest(ACCURACY);
      this.minMillis = minMillis;
    }

    public TDigest getDist() {
      return dist;
    }

    public long getMinMillis() {
      return minMillis;
    }
  }

  public static WavefrontHistogram get(MetricName metricName) {
    return get(Metrics.defaultRegistry(), metricName);
  }

  public static WavefrontHistogram get(MetricsRegistry registry, MetricName metricName) {
    return get(registry, metricName, System::currentTimeMillis);
  }

  @VisibleForTesting
  public static synchronized WavefrontHistogram get(MetricsRegistry registry, MetricName metricName, Supplier<Long> clock) {
    // Awkward construction trying to fit in with Yammer Histograms
    TDigestSample sample = new TDigestSample();
    WavefrontHistogram tDigestHistogram = new WavefrontHistogram(sample, clock);
    sample.set(tDigestHistogram);
    registry.getOrAdd(metricName, tDigestHistogram);
    return tDigestHistogram;
  }

  private WavefrontHistogram(TDigestSample sample, Supplier<Long> millis) {
    super(sample);
    this.millis = millis;
    this.bins = new LinkedList<>();
  }

  public synchronized Collection<MinuteBin> bins(boolean clear) {
    Collection<MinuteBin> result = new ArrayList<>(bins);
    if (clear) bins.clear();
    return result;
  }

  private long minMillis() {
    return (millis.get() / 60000L) * 60000L;
  }

  @Override
  public void update(int value) {
    update((long) value);
  }

  /**
   * Helper to retrieve the current bin. Assumes proper synchronization.
   */
  private MinuteBin getCurrent() {
    long minMillis = minMillis();

    if (bins.isEmpty() || bins.getLast().minMillis != minMillis) {
      bins.add(new MinuteBin(minMillis));
      if (bins.size() > MAX_BINS) {
        bins.removeFirst();
      }
    }
    return bins.getLast();
  }

  /**
   * Bulk-update this histogram with a set of centroids.
   *
   * @param means the centroid values
   * @param counts the centroid weights/sample counts
   */
  public synchronized void bulkUpdate(List<Double> means, List<Integer> counts) {
    if (means != null &&  counts != null) {
      int n = Math.min(means.size(), counts.size());
      MinuteBin current = getCurrent();
      for (int i=0; i<n; ++i) {
        current.dist.add(means.get(i), counts.get(i));
      }
    }
  }

  @Override
  public synchronized void update(long value) {
    getCurrent().dist.add(value);
  }

  @Override
  public double mean() {
    Collection<Centroid> centroids = snapshot().centroids();
    return centroids.stream().mapToDouble(c -> (c.count() * c.mean()) / centroids.size()).sum();
  }

  public synchronized double min() {
    // This is a lie if the winning centroid's weight > 1
    return bins.stream()
        .map(b->b.dist.centroids())
        .mapToDouble(cs-> getFirst(cs, new Centroid(MAX_VALUE)).mean())
        .min().orElse(NaN);
  }

  public synchronized double max() {
    //This is a lie if the winning centroid's weight > 1
    return bins.stream()
        .map(b->b.dist.centroids())
        .mapToDouble(cs-> getLast(cs, new Centroid(MIN_VALUE)).mean())
        .max().orElse(NaN);
  }

  @Override
  public synchronized long count() {
    return bins.stream().mapToLong(bin -> bin.dist.size()).sum();
  }

  @Override
  public synchronized void clear() {
    // More awkwardness
    if (bins != null) {
      bins.clear();
    }
  }

  private synchronized TDigest snapshot() {
    final TDigest snapshot = new AVLTreeDigest(ACCURACY);

    bins.forEach(bin -> snapshot.add(bin.dist));
    return snapshot;
  }

  @Override
  public Snapshot getSnapshot() {
    final TDigest snapshot = snapshot();

    return new Snapshot(new double[0]) {
      @Override
      public double get75thPercentile() {
        return getValue(.75);
      }

      @Override
      public double get95thPercentile() {
        return getValue(.95);
      }

      @Override
      public double get98thPercentile() {
        return getValue(.98);
      }

      @Override
      public double get999thPercentile() {
        return getValue(.999);
      }

      @Override
      public double get99thPercentile() {
        return getValue(.99);
      }

      @Override
      public double getMedian() {
        return getValue(.50);
      }

      @Override
      public double getValue(double quantile) {
        return snapshot.quantile(quantile);
      }

      @Override
      public double[] getValues() {
        return new double[0];
      }

      @Override
      public int size() {
        return (int) snapshot.size();
      }
    };
  }

  @Override
  public <T> void processWith(MetricProcessor<T> metricProcessor, MetricName metricName, T t) throws Exception {
    metricProcessor.processHistogram(metricName, this, t);
  }

  private static class TDigestSample implements Sample {

    private WavefrontHistogram wfHist;

    void set(WavefrontHistogram tdm) {
      this.wfHist = tdm;
    }

    @Override
    public void clear() {
      wfHist.clear();
    }

    @Override
    public int size() {
      return (int) wfHist.count();
    }

    @Override
    public void update(long l) {
      wfHist.update(l);
    }

    @Override
    public Snapshot getSnapshot() {
      return wfHist.getSnapshot();
    }

  }
}
