package com.yammer.metrics.core;

import com.google.common.annotations.VisibleForTesting;

import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.stats.Sample;
import com.yammer.metrics.stats.Snapshot;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.function.Supplier;

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
//    this.sample = sample;
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

  @Override
  public synchronized void update(long value) {

    long minMillis = minMillis();
    if (bins.isEmpty() || bins.getLast().minMillis != minMillis) {
      bins.add(new MinuteBin(minMillis));
      if (bins.size() > MAX_BINS) {
        bins.removeFirst();
      }
    }
    bins.getLast().dist.add(value);
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

  @Override
  public synchronized Snapshot getSnapshot() {
    final TDigest snapshot = new AVLTreeDigest(ACCURACY);

    bins.forEach(bin->snapshot.add(bin.dist));

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
