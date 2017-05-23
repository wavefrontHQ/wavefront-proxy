package com.wavefront.agent.histogram.accumulator;

import com.github.benmanes.caffeine.cache.Cache;
import com.tdunning.math.stats.AgentDigest;
import com.wavefront.agent.histogram.TestUtils;
import com.wavefront.agent.histogram.Utils.HistogramKey;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.truth.Truth.assertThat;

/**
 * Unit tests around {@link AccumulationCache}
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class AccumulationCacheTest {
  private final static long CAPACITY = 2L;
  private final static short COMPRESSION = 100;


  private ConcurrentMap<HistogramKey, AgentDigest> backingStore;
  private Cache<HistogramKey, AgentDigest> cache;
  private Runnable resolveTask;

  private HistogramKey keyA = TestUtils.makeKey("keyA");
  private HistogramKey keyB = TestUtils.makeKey("keyB");
  private HistogramKey keyC = TestUtils.makeKey("keyC");
  private AgentDigest digestA;
  private AgentDigest digestB;
  private AgentDigest digestC;
  private AtomicLong tickerTime;

  @Before
  public void setup() {
    backingStore = new ConcurrentHashMap<>();
    tickerTime = new AtomicLong(0L);
    AccumulationCache ac = new AccumulationCache(backingStore, CAPACITY, tickerTime::get);
    resolveTask = ac.getResolveTask();
    cache = ac.getCache();

    digestA = new AgentDigest(COMPRESSION, 100L);
    digestB = new AgentDigest(COMPRESSION, 1000L);
    digestC = new AgentDigest(COMPRESSION, 10000L);
  }

  @Test
  public void testAddCache() throws ExecutionException {
    cache.put(keyA, digestA);
    assertThat(cache.getIfPresent(keyA)).isEqualTo(digestA);
    assertThat(backingStore.get(keyA)).isNull();
  }

  @Test
  public void testResolveOnNewKey() throws ExecutionException {
    cache.put(keyA, digestA);
    resolveTask.run();
    assertThat(cache.getIfPresent(keyA)).isNull();
    assertThat(backingStore.get(keyA)).isEqualTo(digestA);
  }

  @Test
  public void testResolveOnExistingKey() throws ExecutionException {
    digestA.add(15D, 1);
    digestB.add(15D, 1);
    backingStore.put(keyA, digestB);
    cache.put(keyA, digestA);
    resolveTask.run();
    assertThat(cache.getIfPresent(keyA)).isNull();
    assertThat(backingStore.get(keyA).size()).isEqualTo(2L);
  }

  @Test
  public void testEvictsOnCapacityExceeded() throws ExecutionException {
    // Note: Capacity is 2
    cache.put(keyA, digestA);
    cache.put(keyB, digestB);
    cache.put(keyC, digestC);

    // Force policy application
    cache.cleanUp();

    assertThat(backingStore.size()).isAtLeast(1);
  }
}