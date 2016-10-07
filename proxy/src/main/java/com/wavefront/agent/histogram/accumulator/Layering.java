package com.wavefront.agent.histogram.accumulator;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.tdunning.math.stats.AgentDigest;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.wavefront.agent.histogram.Utils.HistogramKey;

/**
 * Basic layering between Caffeine and some backing store. Dirty/Deleted entries are cached locally. Write-backs can be
 * scheduled via the corresponding writeBackTask. It exposes a KeySetAccessor for traversing the backing store's
 * keyspace.
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class Layering implements CacheWriter<HistogramKey, AgentDigest>, CacheLoader<HistogramKey, AgentDigest> {
  private static final AgentDigest DELETED = new AgentDigest((short)20, 0L);
  private final ConcurrentMap<HistogramKey, AgentDigest> backingStore;
  private final ConcurrentMap<HistogramKey, AgentDigest> dirtyEntries;


  public Layering(ConcurrentMap<HistogramKey, AgentDigest> backingStore) {
    this.backingStore = backingStore;
    this.dirtyEntries = new ConcurrentHashMap<>();
  }

  @Override
  public void write(@Nonnull HistogramKey histogramKey,
                    @Nonnull AgentDigest agentDigest) {
    dirtyEntries.put(histogramKey, agentDigest);
  }

  @Override
  public void delete(@Nonnull HistogramKey histogramKey,
                     @Nullable AgentDigest agentDigest,
                     @Nonnull RemovalCause removalCause) {
    // Only write through on explicit deletes (do exchanges have the
    switch (removalCause) {
      case EXPLICIT:
        dirtyEntries.put(histogramKey, DELETED);
        break;
      default:
    }
  }

  @Override
  public AgentDigest load(@Nonnull HistogramKey key) throws Exception {
    AgentDigest value = dirtyEntries.get(key);
    if (value == null) {
      value = backingStore.get(key);
    } else if (value == DELETED) {
      value = null;
    }
    return value;
  }

  /**
   * Returns a runnable for writing back dirty entries to the backing store.
   */
  public Runnable getWriteBackTask() {
    return new WriteBackTask();
  }

  private class WriteBackTask implements Runnable {

    @Override
    public void run() {
      for (HistogramKey dirtyKey : dirtyEntries.keySet()) {
        AgentDigest dirtyValue = dirtyEntries.remove(dirtyKey);
        if (dirtyValue != null) {
          if (dirtyValue == DELETED) {
            backingStore.remove(dirtyKey);
          } else {
            backingStore.put(dirtyKey, dirtyValue);

          }
        }
      }
    }
  }

  public interface KeySetAccessor {
    /**
     * Keys in the combined set.
     *
     * @return
     */
    Set<HistogramKey> keySet();
  }

  public KeySetAccessor getKeySetAccessor() {
    return backingStore::keySet;
  }
}
