package com.wavefront.agent.histogram.accumulator;

import com.github.benmanes.caffeine.cache.Cache;
import com.tdunning.math.stats.AgentDigest;
import com.wavefront.agent.histogram.TestUtils;
import com.wavefront.agent.histogram.HistogramUtils.HistogramKeyMarshaller;
import com.wavefront.agent.histogram.HistogramKey;

import net.openhft.chronicle.map.ChronicleMap;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import static com.google.common.truth.Truth.assertThat;

/**
 * Unit tests around {@link AccumulationCache}
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class AccumulationCacheTest {
  private final static Logger logger = Logger.getLogger(AccumulationCacheTest.class.getCanonicalName());

  private final static long CAPACITY = 2L;
  private final static short COMPRESSION = 100;


  private ConcurrentMap<HistogramKey, AgentDigest> backingStore;
  private Cache<HistogramKey, AgentDigest> cache;
  private AccumulationCache ac;

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
    AgentDigestFactory agentDigestFactory = new AgentDigestFactory(() -> COMPRESSION, 100,
        tickerTime::get);
    ac = new AccumulationCache(backingStore, agentDigestFactory, CAPACITY, "", tickerTime::get);
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
    ac.flush();
    assertThat(cache.getIfPresent(keyA)).isNull();
    assertThat(backingStore.get(keyA)).isEqualTo(digestA);
  }

  @Test
  public void testResolveOnExistingKey() throws ExecutionException {
    digestA.add(15D, 1);
    digestB.add(15D, 1);
    backingStore.put(keyA, digestB);
    cache.put(keyA, digestA);
    ac.flush();
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

  @Test
  public void testChronicleMapOverflow() {
    ConcurrentMap<HistogramKey, AgentDigest> chronicleMap = ChronicleMap.of(HistogramKey.class, AgentDigest.class).
        keyMarshaller(HistogramKeyMarshaller.get()).
        valueMarshaller(AgentDigest.AgentDigestMarshaller.get()).
        entries(10)
        .averageKeySize(20)
        .averageValueSize(20)
        .maxBloatFactor(10)
        .create();
    AtomicBoolean hasFailed = new AtomicBoolean(false);
    AccumulationCache ac = new AccumulationCache(chronicleMap,
        new AgentDigestFactory(() -> COMPRESSION, 100L, tickerTime::get), 10, "", tickerTime::get,
        () -> hasFailed.set(true));

    for (int i = 0; i < 1000; i++) {
      ac.put(TestUtils.makeKey("key-" + i), digestA);
      ac.flush();
      if (hasFailed.get()) {
        logger.info("Chronicle map overflow detected when adding object #" + i);
        break;
      }
    }
    assertThat(hasFailed.get()).isTrue();
  }
}
