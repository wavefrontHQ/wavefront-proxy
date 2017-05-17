package com.wavefront.agent.histogram.accumulator;

import com.google.common.annotations.VisibleForTesting;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Ticker;
import com.tdunning.math.stats.AgentDigest;
import com.tdunning.math.stats.TDigest;
import com.wavefront.agent.histogram.TimeProvider;
import com.wavefront.agent.histogram.Utils;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import sunnylabs.report.Histogram;

import static com.wavefront.agent.histogram.Utils.HistogramKey;

/**
 * Expose a local cache of limited size along with a task to flush that cache to the backing store.
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class AccumulationCache {
  private final static Logger logger = Logger.getLogger(AccumulationCache.class.getCanonicalName());

  private final Counter binCreatedCounter = Metrics.newCounter(
      new MetricName("histogram.accumulator", "", "bin_created"));
  private final Cache<HistogramKey, AgentDigest> cache;
  private final ConcurrentMap<HistogramKey, AgentDigest> backingStore;
  private final ConcurrentMap<HistogramKey, Long> keyIndex;

  public AccumulationCache(
      final ConcurrentMap<HistogramKey, AgentDigest> backingStore,
      final long cacheSize,
      @Nullable Ticker ticker) {
    this.backingStore = backingStore;
    this.keyIndex = new ConcurrentHashMap<>(backingStore.size());
    if (backingStore.size() > 0) {
      logger.info("Started: Indexing histogram accumulator");
      for (Map.Entry<HistogramKey, AgentDigest> entry : this.backingStore.entrySet()) {
        keyIndex.put(entry.getKey(), entry.getValue().getDispatchTimeMillis());
      }
      logger.info("Finished: Indexing histogram accumulator");
    }
    this.cache = Caffeine.newBuilder()
        .maximumSize(cacheSize)
        .ticker((ticker == null ? Ticker.systemTicker() : ticker))
        .writer(new CacheWriter<HistogramKey, AgentDigest>() {
          @Override
          public void write(@Nonnull HistogramKey key, @Nonnull AgentDigest value) {
            // ignored
          }

          @Override
          public void delete(@Nonnull HistogramKey key, @Nullable AgentDigest value, @Nonnull RemovalCause cause) {
            if (value == null) {
              return;
            }
            // flush out to backing store
            backingStore.merge(key, value, (digestA, digestB) -> {
              if (digestA != null && digestB != null) {
                // Merge both digests
                if (digestA.centroidCount() >= digestB.centroidCount()) {
                  digestA.add(digestB);
                  return digestA;
                } else {
                  digestB.add(digestA);
                  return digestB;
                }
              } else {
                return (digestB == null ? digestA : digestB);
              }
            });
          }
        }).build();
  }

  @VisibleForTesting
  Cache<HistogramKey, AgentDigest> getCache() {
    return cache;
  }

  public void put(HistogramKey key, @NotNull AgentDigest value) {
    cache.asMap().compute(key, (k, v) -> {
      if (v == null) {
        keyIndex.put(key, value.getDispatchTimeMillis());
        return value;
      } else {
        keyIndex.compute(key, (k1, v1) -> (
            v1 != null && v1 > v.getDispatchTimeMillis() ? v1 : v.getDispatchTimeMillis()));
        v.add(value);
        return v;
      }
    });
  }

  public void put(HistogramKey key, Double value, short compression, long ttlMillis) {
    cache.asMap().compute(key, (k, v) -> {
      if (v == null) {
        binCreatedCounter.inc();
        AgentDigest t = new AgentDigest(compression, System.currentTimeMillis() + ttlMillis);
        keyIndex.compute(key, (k1, v1) -> (
            v1 != null && v1 > t.getDispatchTimeMillis() ? v1 : t.getDispatchTimeMillis()
        ));
        t.add(value);
        return t;
      } else {
        keyIndex.compute(key, (k1, v1) -> (
            v1 != null && v1 > v.getDispatchTimeMillis() ? v1 : v.getDispatchTimeMillis()
        ));
        v.add(value);
        return v;
      }
    });
  }

  public void put(HistogramKey key, Histogram value, short compression, long ttlMillis) {
    cache.asMap().compute(key, (k, v) -> {
      if (v == null) {
        binCreatedCounter.inc();
        AgentDigest t = new AgentDigest(compression, System.currentTimeMillis() + ttlMillis);
        keyIndex.compute(key, (k1, v1) -> (
            v1 != null && v1 > t.getDispatchTimeMillis() ? v1 : t.getDispatchTimeMillis()));
        mergeHistogram(t, value);
        return t;
      } else {
        keyIndex.compute(key, (k1, v1) -> (
            v1 != null && v1 > v.getDispatchTimeMillis() ? v1 : v.getDispatchTimeMillis()));
        mergeHistogram(v, value);
        return v;
      }
    });
  }

  public Iterator<HistogramKey> getRipeDigestsIterator(TimeProvider clock) {
    return new Iterator<HistogramKey>() {
      private final Iterator<Map.Entry<HistogramKey, Long>> indexIterator = keyIndex.entrySet().iterator();
      private HistogramKey nextHistogramKey;

      @Override
      public boolean hasNext() {
        while (indexIterator.hasNext()) {
          Map.Entry<Utils.HistogramKey, Long> entry = indexIterator.next();
          if (entry.getValue() < clock.millisSinceEpoch()) {
            nextHistogramKey = entry.getKey();
            return true;
          }
        }
        return false;
      }

      @Override
      public HistogramKey next() {
        return nextHistogramKey;
      }

      @Override
      public void remove() {
        indexIterator.remove();
      }
    };
  }

  public AgentDigest compute(HistogramKey key, BiFunction<? super HistogramKey,? super AgentDigest,
      ? extends AgentDigest> remappingFunction) {
    return backingStore.compute(key, remappingFunction);
  }

  public long size() {
    return backingStore.size();
  }

  private static void mergeHistogram(final TDigest target, final Histogram source) {
    List<Double> means = source.getBins();
    List<Integer> counts = source.getCounts();

    if (means != null && counts != null) {
      int len = Math.min(means.size(), counts.size());

      for (int i = 0; i < len; ++i) {
        Integer count = counts.get(i);
        Double mean = means.get(i);

        if (count != null && count > 0 && mean != null && Double.isFinite(mean)) {
          target.add(mean, count);
        }
      }
    }
  }

  /**
   * Task to merge the contents of this cache with the corresponding backing store.
   *
   * @return the task
   */
  public Runnable getResolveTask() {
    return cache::invalidateAll;
  }
}
