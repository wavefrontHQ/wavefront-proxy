package com.wavefront.agent.histogram.accumulator;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Ticker;
import com.tdunning.math.stats.AgentDigest;

import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.wavefront.agent.histogram.Utils.HistogramKey;

/**
 * Expose a local cache of limited size along with a task to flush that cache to the backing store.
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class AccumulationCache {
  private final Cache<HistogramKey, AgentDigest> cache;

  public AccumulationCache(
      final ConcurrentMap<HistogramKey, AgentDigest> backingStore,
      final long cacheSize,
      @Nullable Ticker ticker) {
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

  public Cache<HistogramKey, AgentDigest> getCache() {
    return cache;
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
