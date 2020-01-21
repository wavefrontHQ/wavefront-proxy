package com.wavefront.agent.histogram.accumulator;

import com.google.common.annotations.VisibleForTesting;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Ticker;
import com.tdunning.math.stats.AgentDigest;
import com.tdunning.math.stats.TDigest;
import com.wavefront.agent.SharedMetricsRegistry;
import com.wavefront.agent.histogram.HistogramKey;
import com.wavefront.common.SharedRateLimitingLogger;
import com.wavefront.common.TimeProvider;
import com.wavefront.agent.histogram.HistogramUtils;
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

import com.yammer.metrics.core.MetricsRegistry;
import wavefront.report.Histogram;

import static com.wavefront.agent.histogram.HistogramUtils.mergeHistogram;

/**
 * Expose a local cache of limited size along with a task to flush that cache to the backing store.
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class AccumulationCache implements Accumulator {
  private static final Logger logger = Logger.getLogger(AccumulationCache.class.getCanonicalName());
  private static final MetricsRegistry sharedRegistry = SharedMetricsRegistry.getInstance();

  private final Counter binCreatedCounter;
  private final Counter binMergedCounter;
  private final Counter cacheBinCreatedCounter;
  private final Counter cacheBinMergedCounter;
  private final Counter flushedCounter;
  private final Counter cacheOverflowCounter = Metrics.newCounter(
      new MetricName("histogram.accumulator.cache", "", "size_exceeded"));
  private final boolean cacheEnabled;
  private final Cache<HistogramKey, AgentDigest> cache;
  private final ConcurrentMap<HistogramKey, AgentDigest> backingStore;
  private final AgentDigestFactory agentDigestFactory;

  /**
   * In-memory index for dispatch timestamps to avoid iterating the backing store map, which is an
   * expensive operation, as it requires value objects to be de-serialized first.
   */
  private final ConcurrentMap<HistogramKey, Long> keyIndex;

  /**
   * Constructs a new AccumulationCache instance around {@code backingStore} and builds an in-memory
   * index maintaining dispatch times in milliseconds for all HistogramKeys in the backingStore.
   * Setting cacheSize to 0 disables in-memory caching so the cache only maintains the dispatch
   * time index.
   *
   * @param backingStore       a {@code ConcurrentMap} storing {@code AgentDigests}
   * @param agentDigestFactory a factory that generates {@code AgentDigests} with pre-defined
   *                           compression level and TTL time
   * @param cacheSize          maximum size of the cache
   * @param ticker             a nanosecond-precision time source
   */
  public AccumulationCache(
      final ConcurrentMap<HistogramKey, AgentDigest> backingStore,
      final AgentDigestFactory agentDigestFactory,
      final long cacheSize,
      String metricPrefix,
      @Nullable Ticker ticker) {
    this(backingStore, agentDigestFactory, cacheSize, metricPrefix, ticker, null);
  }

  /**
   * Constructs a new AccumulationCache instance around {@code backingStore} and builds an in-memory
   * index maintaining dispatch times in milliseconds for all HistogramKeys in the backingStore.
   * Setting cacheSize to 0 disables in-memory caching, so the cache only maintains the dispatch
   * time index.
   *
   * @param backingStore       a {@code ConcurrentMap} storing {@code AgentDigests}
   * @param agentDigestFactory a factory that generates {@code AgentDigests} with pre-defined
   *                           compression level and TTL time
   * @param cacheSize          maximum size of the cache
   * @param ticker             a nanosecond-precision time source
   * @param onFailure          a {@code Runnable} that is invoked when backing store overflows
   */
  @VisibleForTesting
  protected AccumulationCache(
      final ConcurrentMap<HistogramKey, AgentDigest> backingStore,
      final AgentDigestFactory agentDigestFactory,
      final long cacheSize,
      String metricPrefix,
      @Nullable Ticker ticker,
      @Nullable Runnable onFailure) {
    this.backingStore = backingStore;
    this.agentDigestFactory = agentDigestFactory;
    this.cacheEnabled = cacheSize > 0;
    this.binCreatedCounter = Metrics.newCounter(new MetricName(metricPrefix, "", "bin_created"));
    this.binMergedCounter = Metrics.newCounter(new MetricName(metricPrefix, "", "bin_merged"));
    MetricsRegistry metricsRegistry = cacheEnabled ? Metrics.defaultRegistry() : sharedRegistry;
    this.cacheBinCreatedCounter = metricsRegistry.newCounter(new MetricName(metricPrefix + ".cache",
        "", "bin_created"));
    this.cacheBinMergedCounter = metricsRegistry.newCounter(new MetricName(metricPrefix + ".cache",
        "", "bin_merged"));
    this.flushedCounter = Metrics.newCounter(new MetricName(metricPrefix + ".cache", "",
        "flushed"));
    this.keyIndex = new ConcurrentHashMap<>(backingStore.size());
    final Runnable failureHandler = onFailure == null ? new AccumulationCacheMonitor() : onFailure;
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
          public void delete(@Nonnull HistogramKey key, @Nullable AgentDigest value,
                             @Nonnull RemovalCause cause) {
            if (value == null) {
              return;
            }
            flushedCounter.inc();
            if (cause == RemovalCause.SIZE && cacheEnabled) cacheOverflowCounter.inc();
            try {
              // flush out to backing store
              AgentDigest merged = backingStore.merge(key, value, (digestA, digestB) -> {
                // Merge both digests
                if (digestA.centroidCount() >= digestB.centroidCount()) {
                  digestA.add(digestB);
                  return digestA;
                } else {
                  digestB.add(digestA);
                  return digestB;
                }
              });
              if (merged == value) {
                binCreatedCounter.inc();
              } else {
                binMergedCounter.inc();
              }
            } catch (IllegalStateException e) {
              if (e.getMessage().contains("Attempt to allocate")) {
                failureHandler.run();
              } else {
                throw e;
              }
            }
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
  @Override
  public void put(HistogramKey key, @Nonnull AgentDigest value) {
    cache.asMap().compute(key, (k, v) -> {
      if (v == null) {
        if (cacheEnabled) cacheBinCreatedCounter.inc();
        keyIndex.put(key, value.getDispatchTimeMillis());
        return value;
      } else {
        if (cacheEnabled) cacheBinMergedCounter.inc();
        keyIndex.compute(key, (k1, v1) -> (
            v1 != null && v1 < v.getDispatchTimeMillis() ? v1 : v.getDispatchTimeMillis()));
        v.add(value);
        return v;
      }
    });
  }

  /**
   * Update {@link AgentDigest} in the cache with a double value. If such {@code AgentDigest} does
   * not exist for the specified key, it will be created using {@link AgentDigestFactory}
   *
   * @param key histogram key
   * @param value value to be merged into the {@code AgentDigest}
   */
  @Override
  public void put(HistogramKey key, double value) {
    cache.asMap().compute(key, (k, v) -> {
      if (v == null) {
        if (cacheEnabled) cacheBinCreatedCounter.inc();
        AgentDigest t = agentDigestFactory.newDigest();
        keyIndex.compute(key, (k1, v1) -> (
            v1 != null && v1 < t.getDispatchTimeMillis() ? v1 : t.getDispatchTimeMillis()
        ));
        t.add(value);
        return t;
      } else {
        if (cacheEnabled) cacheBinMergedCounter.inc();
        keyIndex.compute(key, (k1, v1) -> (
            v1 != null && v1 < v.getDispatchTimeMillis() ? v1 : v.getDispatchTimeMillis()
        ));
        v.add(value);
        return v;
      }
    });
  }

  /**
   * Update {@link AgentDigest} in the cache with a {@code Histogram} value. If such
   * {@code AgentDigest} does not exist for the specified key, it will be created
   * using {@link AgentDigestFactory}.
   *
   * @param key histogram key
   * @param value a {@code Histogram} to be merged into the {@code AgentDigest}
   */
  @Override
  public void put(HistogramKey key, Histogram value) {
    cache.asMap().compute(key, (k, v) -> {
      if (v == null) {
        if (cacheEnabled) cacheBinCreatedCounter.inc();
        AgentDigest t = agentDigestFactory.newDigest();
        keyIndex.compute(key, (k1, v1) -> (
            v1 != null && v1 < t.getDispatchTimeMillis() ? v1 : t.getDispatchTimeMillis()));
        mergeHistogram(t, value);
        return t;
      } else {
        if (cacheEnabled) cacheBinMergedCounter.inc();
        keyIndex.compute(key, (k1, v1) -> (
            v1 != null && v1 < v.getDispatchTimeMillis() ? v1 : v.getDispatchTimeMillis()));
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
  @Override
  public Iterator<HistogramKey> getRipeDigestsIterator(TimeProvider clock) {
    return new Iterator<HistogramKey>() {
      private final Iterator<Map.Entry<HistogramKey, Long>> indexIterator = keyIndex.entrySet().iterator();
      private HistogramKey nextHistogramKey;

      @Override
      public boolean hasNext() {
        while (indexIterator.hasNext()) {
          Map.Entry<HistogramKey, Long> entry = indexIterator.next();
          if (entry.getValue() < clock.currentTimeMillis()) {
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
  @Override
  public AgentDigest compute(HistogramKey key, BiFunction<? super HistogramKey,? super AgentDigest,
      ? extends AgentDigest> remappingFunction) {
    return backingStore.compute(key, remappingFunction);
  }

  /**
   * Returns the number of items in the storage behind the cache
   *
   * @return number of items
   */
  @Override
  public long size() {
    return backingStore.size();
  }

  /**
   * Merge the contents of this cache with the corresponding backing store.
   */
  @Override
  public void flush() {
    cache.invalidateAll();
  }

  private static class AccumulationCacheMonitor implements Runnable {
    private final Logger throttledLogger = new SharedRateLimitingLogger(logger,
        "accumulator-failure", 1.0d);
    private Counter failureCounter;

    @Override
    public void run() {
      if (failureCounter == null) {
        failureCounter = Metrics.newCounter(new MetricName("histogram.accumulator", "", "failure"));
      }
      failureCounter.inc();
      throttledLogger.severe("CRITICAL: Histogram accumulator overflow - " +
          "losing histogram data!!! Accumulator size configuration setting is " +
          "not appropriate for the current workload, please increase the value " +
          "as appropriate and restart the proxy!");
    }
  }
}
