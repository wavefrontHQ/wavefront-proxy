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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

  private final ConcurrentMap<Long, LinkedList<MinuteBin>> perThreadHistogramBins = new ConcurrentHashMap<>();

  private WavefrontHistogram(TDigestSample sample, Supplier<Long> millis) {
    super(sample);
    this.millis = millis;
  }

  public static WavefrontHistogram get(MetricName metricName) {
    return get(Metrics.defaultRegistry(), metricName);
  }

  public static WavefrontHistogram get(MetricsRegistry registry, MetricName metricName) {
    return get(registry, metricName, System::currentTimeMillis);
  }

  @VisibleForTesting
  public static synchronized WavefrontHistogram get(MetricsRegistry registry,
                                                    MetricName metricName,
                                                    Supplier<Long> clock) {
    // Awkward construction trying to fit in with Yammer Histograms
    TDigestSample sample = new TDigestSample();
    WavefrontHistogram tDigestHistogram = new WavefrontHistogram(sample, clock);
    sample.set(tDigestHistogram);
    return registry.getOrAdd(metricName, tDigestHistogram);
  }

  /**
   * Aggregates all the bins prior to the current minute
   * This is because threads might be updating the current minute bin while the bins() method is invoked
   *
   * @param clear if set to true, will clear the older bins
   * @return returns aggregated collection of all the bins prior to the current minute
   */
  public List<MinuteBin> bins(boolean clear) {
    List<MinuteBin> result = new ArrayList<>();
    final long cutoffMillis = minMillis();
    perThreadHistogramBins.values().stream().flatMap(List::stream).
        filter(i -> i.getMinMillis() < cutoffMillis).forEach(result::add);

    if (clear) {
      clearPriorCurrentMinuteBin(cutoffMillis);
    }

    return result;
  }

  private long minMillis() {
    long currMillis;
    if (millis == null) {
      // happens because WavefrontHistogram.get() invokes the super() Histogram constructor
      // which invokes clear() method which in turn invokes this method
      currMillis = System.currentTimeMillis();
    } else {
      currMillis = millis.get();
    }
    return (currMillis / 60000L) * 60000L;
  }

  @Override
  public void update(int value) {
    update((double) value);
  }

  /**
   * Helper to retrieve the current bin. Will be invoked per thread.
   */
  private MinuteBin getCurrent() {
    long key = Thread.currentThread().getId();
    LinkedList<MinuteBin> bins = perThreadHistogramBins.get(key);
    if (bins == null) {
      bins = new LinkedList<>();
      LinkedList<MinuteBin> existing = perThreadHistogramBins.putIfAbsent(key, bins);
      if (existing != null) {
        bins = existing;
      }
    }

    long currMinMillis = minMillis();

    // bins with clear == true flag will drain (CONSUMER) the list,
    // so synchronize the access to the respective 'bins' list
    synchronized (bins) {
      if (bins.isEmpty() || bins.getLast().minMillis != currMinMillis) {
        bins.add(new MinuteBin(currMinMillis));
        if (bins.size() > MAX_BINS) {
          bins.removeFirst();
        }
      }
      return bins.getLast();
    }
  }

  /**
   * Bulk-update this histogram with a set of centroids.
   *
   * @param means  the centroid values
   * @param counts the centroid weights/sample counts
   */
  public void bulkUpdate(List<Double> means, List<Integer> counts) {
    if (means != null && counts != null) {
      int n = Math.min(means.size(), counts.size());
      MinuteBin current = getCurrent();
      for (int i = 0; i < n; ++i) {
        current.dist.add(means.get(i), counts.get(i));
      }
    }
  }

  public void update(double value) {
    getCurrent().dist.add(value);
  }

  @Override
  public void update(long value) {
    update((double)value);
  }

  @Override
  public double mean() {
    Collection<Centroid> centroids = snapshot().centroids();
    return centroids.size() == 0 ?
        Double.NaN :
        centroids.stream().mapToDouble(c -> (c.count() * c.mean()) / centroids.size()).sum();
  }

  public double min() {
    // This is a lie if the winning centroid's weight > 1
    return perThreadHistogramBins.values().stream().flatMap(List::stream).map(b -> b.dist.centroids()).
        mapToDouble(cs -> getFirst(cs, new Centroid(MAX_VALUE)).mean()).min().orElse(NaN);
  }

  public double max() {
    //This is a lie if the winning centroid's weight > 1
    return perThreadHistogramBins.values().stream().flatMap(List::stream).map(b -> b.dist.centroids()).
        mapToDouble(cs -> getLast(cs, new Centroid(MIN_VALUE)).mean()).max().orElse(NaN);
  }

  @Override
  public long count() {
    return perThreadHistogramBins.values().stream().flatMap(List::stream).mapToLong(bin -> bin.dist.size()).sum();
  }

  @Override
  public double sum() {
    return Double.NaN;
  }

  @Override
  public double stdDev() {
    return Double.NaN;
  }

  /**
   * Note - We override the behavior of the clear() method.
   * In the super class, we would clear all the recorded values.
   */
  @Override
  public void clear() {
    // More awkwardness
    clearPriorCurrentMinuteBin(minMillis());
  }

  private void clearPriorCurrentMinuteBin(long cutoffMillis) {
    if (perThreadHistogramBins == null) {
      // will happen if WavefrontHistogram.super() constructor will be invoked
      // before WavefrontHistogram object is fully instantiated,
      // which will be invoke clear() method
      return;
    }
    for (LinkedList bins : perThreadHistogramBins.values()) {
      // getCurrent() method will add (PRODUCER) item to the bins list, so synchronize the access
      synchronized (bins) {
        Iterator<MinuteBin> iter = bins.iterator();
        while (iter.hasNext()) {
          if (iter.next().getMinMillis() < cutoffMillis) {
            iter.remove();
          }
        }
        // TODO - what happens if the list is empty ?? remove from hashmap ??
      }
    }
  }

  // TODO - how to ensure thread safety? do we care?
  private TDigest snapshot() {
    final TDigest snapshot = new AVLTreeDigest(ACCURACY);
    perThreadHistogramBins.values().stream().flatMap(List::stream).forEach(bin -> snapshot.add(bin.dist));
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
