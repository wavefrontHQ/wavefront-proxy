package com.codahale.metrics;

import com.google.common.primitives.Doubles;
import com.tdunning.math.stats.Centroid;
import com.wavefront.common.MinuteBin;
import org.hamcrest.collection.IsMapContaining;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Basic unit tests around {@link WavefrontHistogram}
 *
 * @author Han Zhang (zhanghan@vmware.com)
 */
public class WavefrontHistogramTest {

  private static final double DELTA = 1e-1;

  private static MetricRegistry metricRegistry;
  private static AtomicLong clock;

  private static WavefrontHistogram pow10, inc100, inc1000;

  private static WavefrontHistogram createPow10Histogram(String metricName) {
    WavefrontHistogram wh = WavefrontHistogram.get(metricRegistry, metricName, clock::get);
    wh.update(0.1);
    wh.update(1);
    wh.update(10);
    wh.update(10);
    wh.update(100);
    wh.update(1000);
    wh.update(10000);
    wh.update(10000);
    wh.update(100000);

    return wh;
  }

  private Map<Double, Integer> distributionToMap(List<MinuteBin> bins) {
    Map<Double, Integer> map = new HashMap<>();

    for (MinuteBin minuteBin : bins) {
      StringBuilder sb = new StringBuilder();
      for (Centroid c : minuteBin.getDist().centroids()) {
        map.put(c.mean(), map.getOrDefault(c.mean(), 0) + c.count());
      }
    }

    return map;
  }

  @BeforeClass
  public static void setUp() {
    metricRegistry = new MetricRegistry();
    clock = new AtomicLong(System.currentTimeMillis());

    // WavefrontHistogram with values that are powers of 10
    pow10 = createPow10Histogram("test.hist.pow10");

    // WavefrontHistogram with a value for each integer from 1 to 100
    inc100 = WavefrontHistogram.get(metricRegistry, "test.hist.inc100", clock::get);
    for (int i = 1; i <= 100; i++) {
      inc100.update(i);
    }

    // WavefrontHistogram with a value for each integer from 1 to 1000
    inc1000 = WavefrontHistogram.get(metricRegistry, "test.hist.inc1000", clock::get);
    for (int i = 1; i <= 1000; i++) {
      inc1000.update(i);
    }

    // Simulate that 1 min has passed so that values prior to the current min are ready to be read
    clock.addAndGet(60000L + 1);
  }

  @Test
  public void testDistribution() {
    WavefrontHistogram wh = createPow10Histogram("test.hist.dist");
    clock.addAndGet(60000L + 1);

    List<MinuteBin> bins = wh.bins(true);
    Map<Double, Integer> map = distributionToMap(bins);

    assertEquals(7, map.size());
    assertThat(map, IsMapContaining.hasEntry(0.1, 1));
    assertThat(map, IsMapContaining.hasEntry(1.0, 1));
    assertThat(map, IsMapContaining.hasEntry(10.0, 2));
    assertThat(map, IsMapContaining.hasEntry(100.0, 1));
    assertThat(map, IsMapContaining.hasEntry(1000.0, 1));
    assertThat(map, IsMapContaining.hasEntry(10000.0, 2));
    assertThat(map, IsMapContaining.hasEntry(100000.0, 1));
  }

  @Test
  public void testClear() {
    WavefrontHistogram wh = createPow10Histogram("test.hist.clear");
    clock.addAndGet(60000L + 1);

    wh.bins(true);  // clears bins
    List<MinuteBin> bins = wh.bins(true);
    Map<Double, Integer> map = distributionToMap(bins);

    assertEquals(0, map.size());
  }

  @Test
  public void testBulkUpdate() {
    WavefrontHistogram wh = WavefrontHistogram.get(metricRegistry, "test.hist.bulkupdate", clock::get);
    wh.bulkUpdate(Doubles.asList(24.2, 84.35, 1002), Arrays.asList(80, 1, 9));
    clock.addAndGet(60000L + 1);

    List<MinuteBin> bins = wh.bins(true);
    Map<Double, Integer> map = distributionToMap(bins);

    assertEquals(3, map.size());
    assertThat(map, IsMapContaining.hasEntry(24.2, 80));
    assertThat(map, IsMapContaining.hasEntry(84.35, 1));
    assertThat(map, IsMapContaining.hasEntry(1002.0, 9));
  }

  @Test
  public void testCount() {
    assertEquals(9, pow10.getCount());
  }

  @Test
  public void testSnapshotSize() {
    Snapshot snapshot = pow10.getSnapshot();
    assertEquals(9, snapshot.size());
  }

  @Test
  public void testSnapshotMin() {
    Snapshot snapshot = inc100.getSnapshot();
    assertEquals(1, snapshot.getMin());
  }

  @Test
  public void testSnapshotMax() {
    Snapshot snapshot = pow10.getSnapshot();
    assertEquals(100000, snapshot.getMax());
  }

  @Test
  public void testSnapshotMean() {
    Snapshot snapshot = pow10.getSnapshot();
    assertEquals(13457.9, snapshot.getMean(), DELTA);
  }

  @Test
  public void testSnapshotMedian() {
    Snapshot snapshot = pow10.getSnapshot();
    assertEquals(100, snapshot.getMedian(), DELTA);
  }

  @Test
  public void testSnapshotValue() {
    Snapshot snapshot = inc100.getSnapshot();
    assertEquals(25.5, snapshot.getValue(.25), DELTA);
  }

  @Test
  public void testSnapshot75thPercentile() {
    Snapshot snapshot = inc100.getSnapshot();
    assertEquals(75.5, snapshot.get75thPercentile(), DELTA);
  }

  @Test
  public void testSnapshot95thPercentile() {
    Snapshot snapshot = inc100.getSnapshot();
    assertEquals(95.5, snapshot.get95thPercentile(), DELTA);
  }

  @Test
  public void testSnapshot98thPercentile() {
    Snapshot snapshot = inc100.getSnapshot();
    assertEquals(98.5, snapshot.get98thPercentile(), DELTA);
  }

  @Test
  public void testSnapshot99thPercentile() {
    Snapshot snapshot = inc100.getSnapshot();
    assertEquals(99.5, snapshot.get99thPercentile(), DELTA);
  }

  @Test
  public void testSnapshot999thPercentile() {
    Snapshot snapshot = inc1000.getSnapshot();
    assertEquals(999.5, snapshot.get999thPercentile(), DELTA);
  }
}
