package com.wavefront.agent.histogram.accumulator;


import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.tdunning.math.stats.AgentDigest;
import com.wavefront.agent.histogram.TestUtils;
import com.wavefront.agent.histogram.Utils.HistogramKey;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.truth.Truth.assertThat;

/**
 * Unit tests around Layering
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class LayeringTest {
  private final static short COMPRESSION = 100;

  private ConcurrentMap<HistogramKey, AgentDigest> backingStore;
  private LoadingCache<HistogramKey, AgentDigest> cache;
  private Runnable writeBackTask;
  private HistogramKey keyA = TestUtils.makeKey("keyA");
  private AgentDigest digestA = new AgentDigest(COMPRESSION, 100L);
  private AgentDigest digestB = new AgentDigest(COMPRESSION, 1000L);
  private AgentDigest digestC = new AgentDigest(COMPRESSION, 10000L);
  private AtomicLong tickerTime;

  @Before
  public void setup() {
    backingStore = new ConcurrentHashMap<>();
    Layering layering = new Layering(backingStore);
    writeBackTask = layering.getWriteBackTask();
    tickerTime = new AtomicLong(0L);
    cache = Caffeine.newBuilder()
        .expireAfterAccess(1, TimeUnit.SECONDS)
        .writer(layering)
        .ticker(() -> tickerTime.get()).build(layering);
  }

  @Test
  public void testNoWriteThrough() throws ExecutionException {
    cache.put(keyA, digestA);
    assertThat(cache.get(keyA)).isEqualTo(digestA);
    assertThat(backingStore.get(keyA)).isNull();
  }

  @Test
  public void testWriteBack() throws ExecutionException {
    cache.put(keyA, digestA);
    writeBackTask.run();
    assertThat(cache.get(keyA)).isEqualTo(digestA);
    assertThat(backingStore.get(keyA)).isEqualTo(digestA);
  }

  @Test
  public void testOnlyWriteBackLastChange() throws ExecutionException {
    cache.put(keyA, digestA);
    writeBackTask.run();
    assertThat(cache.get(keyA)).isEqualTo(digestA);
    assertThat(backingStore.get(keyA)).isEqualTo(digestA);

    cache.put(keyA, digestB);
    cache.put(keyA, digestC);

    assertThat(backingStore.get(keyA)).isEqualTo(digestA);

    writeBackTask.run();
    assertThat(backingStore.get(keyA)).isEqualTo(digestC);
  }

  @Test
  public void testBasicRead() throws ExecutionException {
    backingStore.put(keyA, digestA);
    assertThat(cache.get(keyA)).isEqualTo(digestA);
  }

  @Test
  public void testReadDirtyKeyAfterEviction() throws ExecutionException {
    // Write a change
    cache.put(keyA, digestA);
    assertThat(cache.get(keyA)).isEqualTo(digestA);
    // Let the entry expire
    tickerTime.addAndGet(TimeUnit.SECONDS.toNanos(2L));
    cache.cleanUp();
    assertThat(cache.estimatedSize()).isEqualTo(0L);
    // Ensure we can still read the unpersisted change
    assertThat(cache.get(keyA)).isEqualTo(digestA);
  }

  @Test
  public void testExplicitDelete() throws ExecutionException {
    backingStore.put(keyA, digestA);

    cache.get(keyA);
    cache.asMap().remove(keyA);
    // Propagate
    writeBackTask.run();
    assertThat(backingStore.size()).isEqualTo(0L);
  }
}
