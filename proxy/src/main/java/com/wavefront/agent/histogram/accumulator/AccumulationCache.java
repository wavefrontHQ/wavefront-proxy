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

import wavefront.report.Histogram;

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

  /**
   * In-memory index for dispatch timestamps to avoid iterating the backing store map, which is an expensive
   * operation, as it requires value objects to be de-serialized first
   */
  private final ConcurrentMap<HistogramKey, Long> keyIndex;

  /**
   * Constructs a new AccumulationCache instance around {@code backingStore} and builds an in-memory index maintaining
   * dispatch times in milliseconds for all HistogramKeys in backingStore
   * Setting cacheSize to 0 disables in-memory caching so the cache only maintains the dispatch time index.
   *
   * @param backingStore a {@code ConcurrentMap} storing AgentDigests
   * @param cacheSize maximum size of the cache
   * @param ticker a nanosecond-precision time source (to
   */
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
        .ticker(ticker == null ? Ticker.systemTicker() : ticker)
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

  /**
   * Update {@code AgentDigest} in the cache with another {@code AgentDigest}.
   *
   * @param key histogram key
   * @param value {@code AgentDigest} to be merged
   */
  public void put(HistogramKey key, @Nonnull AgentDigest value) {
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

  /**
   * Update {@code AgentDigest} in the cache with a double value. If such {@code AgentDigest} does not exist for
   * the specified key, it will be created with the specified compression and ttlMillis settings.
   *
   * @param key histogram key
   * @param value value to be merged into the {@code AgentDigest}
   * @param compression default compression level for new bins
   * @param ttlMillis default time-to-dispatch for new bins
   */
  public void put(HistogramKey key, double value, short compression, long ttlMillis) {
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

  /**
   * Update {@code AgentDigest} in the cache with a {@code Histogram} value. If such {@code AgentDigest} does not exist
   * for the specified key, it will be created with the specified compression and ttlMillis settings.
   *
   * @param key histogram key
   * @param value a {@code Histogram} to be merged into the {@code AgentDigest}
   * @param compression default compression level for new bins
   * @param ttlMillis default time-to-dispatch in milliseconds for new bins
   */
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

  /**
   * Returns an iterator over "ripe" digests ready to be shipped

   * @param clock a millisecond-precision epoch time source
   * @return an iterator over "ripe" digests ready to be shipped
   */
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

  /**
   * Attempts to compute a mapping for the specified key and its current mapped value
   * (or null if there is no current mapping).
   *
   * @param key               key with which the specified value is to be associated
   * @param remappingFunction the function to compute a value
   * @return                  the new value associated with the specified key, or null if none
   */
  public AgentDigest compute(HistogramKey key, BiFunction<? super HistogramKey,? super AgentDigest,
      ? extends AgentDigest> remappingFunction) {
    return backingStore.compute(key, remappingFunction);
  }

  /**
   * Returns the number of items in the storage behind the cache
   *
   * @return number of items
   */
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
