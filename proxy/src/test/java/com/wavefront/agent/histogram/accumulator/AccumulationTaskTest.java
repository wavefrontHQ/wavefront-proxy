package com.wavefront.agent.histogram.accumulator;

import com.google.common.collect.Lists;

import com.squareup.tape.InMemoryObjectQueue;
import com.squareup.tape.ObjectQueue;
import com.tdunning.math.stats.AgentDigest;
import com.wavefront.agent.PointHandler;
import com.wavefront.agent.Validation;
import com.wavefront.agent.histogram.Utils;
import com.wavefront.agent.histogram.Utils.HistogramKey;
import com.wavefront.ingester.GraphiteDecoder;
import com.wavefront.ingester.HistogramDecoder;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import jersey.repackaged.com.google.common.collect.ImmutableList;
import sunnylabs.report.ReportPoint;

import static com.google.common.truth.Truth.assertThat;
import static com.wavefront.agent.histogram.TestUtils.DEFAULT_TIME_MILLIS;
import static com.wavefront.agent.histogram.TestUtils.DEFAULT_VALUE;
import static com.wavefront.agent.histogram.TestUtils.makeKey;
import static com.wavefront.agent.histogram.Utils.Granularity.*;

/**
 * Unit tests around {@link AccumulationTask}
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class AccumulationTaskTest {
  private ObjectQueue<List<String>> in;
  private ConcurrentMap<HistogramKey, AgentDigest> out;
  private List<String> badPointsOut;
  private AccumulationTask eventSubject, histoSubject;
  private AccumulationCache cache;
  private final static long TTL = 30L;
  private final static short COMPRESSION = 100;

  private String lineA = "minKeyA " + DEFAULT_VALUE + " " + DEFAULT_TIME_MILLIS;
  private String lineB = "minKeyB " + DEFAULT_VALUE + " " + DEFAULT_TIME_MILLIS;
  private String lineC = "minKeyC " + DEFAULT_VALUE + " " + DEFAULT_TIME_MILLIS;

  private String histoMinLineA = "!M " + DEFAULT_TIME_MILLIS + " #2 20 #5 70 #6 123 minKeyA";
  private String histoMinLineB = "!M " + DEFAULT_TIME_MILLIS + " #1 10 #2 1000 #3 1000 minKeyB";
  private String histoHourLineA = "!H " + DEFAULT_TIME_MILLIS + " #11 10 #9 1000 #17 1000 hourKeyA";
  private String histoDayLineA = "!D " + DEFAULT_TIME_MILLIS + " #5 10 #5 1000 #5 1000 dayKeyA";


  private HistogramKey minKeyA = makeKey("minKeyA");
  private HistogramKey minKeyB = makeKey("minKeyB");
  private HistogramKey minKeyC = makeKey("minKeyC");
  private HistogramKey hourKeyA = makeKey("hourKeyA", HOUR);
  private HistogramKey dayKeyA = makeKey("dayKeyA", DAY);

  @Before
  public void setUp() throws Exception {
    AtomicInteger timeMillis = new AtomicInteger(0);
    in = new InMemoryObjectQueue<>();
    out = new ConcurrentHashMap<>();
    cache = new AccumulationCache(out, 0, timeMillis::get);
    badPointsOut = Lists.newArrayList();

    PointHandler pointHandler = new PointHandler() {
      @Override
      public void reportPoint(ReportPoint point, String debugLine) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void reportPoints(List<ReportPoint> points) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void handleBlockedPoint(String pointLine) {
        badPointsOut.add(pointLine);
      }
    };

    eventSubject = new AccumulationTask(
        in,
        cache,
        new GraphiteDecoder("unknown", ImmutableList.of()),
        pointHandler,
        Validation.Level.NUMERIC_ONLY,
        TTL,
        MINUTE,
        COMPRESSION);

    histoSubject = new AccumulationTask(
        in,
        cache,
        new HistogramDecoder(),
        pointHandler,
        Validation.Level.NUMERIC_ONLY,
        TTL,
        MINUTE,
        COMPRESSION);
  }

  @Test
  public void testSingleLine() {
    in.add(ImmutableList.of(lineA));

    eventSubject.run();
    cache.getResolveTask().run();

    assertThat(badPointsOut).hasSize(0);
    assertThat(out.get(minKeyA)).isNotNull();
    assertThat(out.get(minKeyA).size()).isEqualTo(1);
  }

  @Test
  public void testMultipleLines() {
    in.add(ImmutableList.of(lineA, lineB, lineC));

    eventSubject.run();
    cache.getResolveTask().run();

    assertThat(badPointsOut).hasSize(0);
    assertThat(out.get(minKeyA)).isNotNull();
    assertThat(out.get(minKeyA).size()).isEqualTo(1);
    assertThat(out.get(minKeyB)).isNotNull();
    assertThat(out.get(minKeyB).size()).isEqualTo(1);
    assertThat(out.get(minKeyC)).isNotNull();
    assertThat(out.get(minKeyC).size()).isEqualTo(1);
  }

  @Test
  public void testMultipleTimes() {
    in.add(ImmutableList.of(
        "min0 1 " + (DEFAULT_TIME_MILLIS - 60000),
        "min0 1 " + (DEFAULT_TIME_MILLIS - 60000),
        "min1 1 " + DEFAULT_TIME_MILLIS));

    eventSubject.run();
    cache.getResolveTask().run();

    HistogramKey min0 = Utils.makeKey(ReportPoint.newBuilder()
        .setMetric("min0")
        .setTimestamp(DEFAULT_TIME_MILLIS - 60000)
        .setValue(1).build(), MINUTE);

    HistogramKey min1 = Utils.makeKey(ReportPoint.newBuilder()
        .setMetric("min1")
        .setTimestamp(DEFAULT_TIME_MILLIS)
        .setValue(1).build(), MINUTE);

    assertThat(badPointsOut).hasSize(0);
    assertThat(out.get(min0)).isNotNull();
    assertThat(out.get(min0).size()).isEqualTo(2);
    assertThat(out.get(min1)).isNotNull();
    assertThat(out.get(min1).size()).isEqualTo(1);
  }

  @Test
  public void testAccumulation() {
    in.add(ImmutableList.of(lineA, lineA, lineA));

    eventSubject.run();
    cache.getResolveTask().run();

    assertThat(out.get(minKeyA)).isNotNull();
    assertThat(out.get(minKeyA).size()).isEqualTo(3);
  }

  @Test
  public void testAccumulationNoTime() {
    in.add(ImmutableList.of("noTimeKey 100"));

    eventSubject.run();
    cache.getResolveTask().run();

    assertThat(out).hasSize(1);
  }

  @Test
  public void testAccumulateWithBadLine() {
    in.add(ImmutableList.of("This is not really a valid sample", lineA, lineA, lineA));

    eventSubject.run();
    cache.getResolveTask().run();

    assertThat(badPointsOut).hasSize(1);
    assertThat(badPointsOut.get(0)).contains("This is not really a valid sample");

    assertThat(out.get(minKeyA)).isNotNull();
    assertThat(out.get(minKeyA).size()).isEqualTo(3);
  }

  @Test
  public void testHistogramLine() {
    in.add(ImmutableList.of(histoMinLineA));

    histoSubject.run();
    cache.getResolveTask().run();

    assertThat(badPointsOut).hasSize(0);
    assertThat(out.get(minKeyA)).isNotNull();
    assertThat(out.get(minKeyA).size()).isEqualTo(13);
  }

  @Test
  public void testHistogramAccumulation() {
    in.add(ImmutableList.of(histoMinLineA, histoMinLineA, histoMinLineA));

    histoSubject.run();
    cache.getResolveTask().run();

    assertThat(badPointsOut).hasSize(0);
    assertThat(out.get(minKeyA)).isNotNull();
    assertThat(out.get(minKeyA).size()).isEqualTo(39);
  }

  @Test
  public void testHistogramAccumulationMultipleGranularities() {
    in.add(ImmutableList.of(histoMinLineA, histoHourLineA, histoDayLineA));

    histoSubject.run();
    cache.getResolveTask().run();

    assertThat(badPointsOut).hasSize(0);
    assertThat(out.get(minKeyA)).isNotNull();
    assertThat(out.get(minKeyA).size()).isEqualTo(13);
    assertThat(out.get(hourKeyA)).isNotNull();
    assertThat(out.get(hourKeyA).size()).isEqualTo(37);
    assertThat(out.get(dayKeyA)).isNotNull();
    assertThat(out.get(dayKeyA).size()).isEqualTo(15);
  }

  @Test
  public void testHistogramMultiBinAccumulation() {
    in.add(ImmutableList.of(histoMinLineA, histoMinLineB, histoMinLineA, histoMinLineB));

    histoSubject.run();
    cache.getResolveTask().run();

    assertThat(badPointsOut).hasSize(0);
    assertThat(out.get(minKeyA)).isNotNull();
    assertThat(out.get(minKeyA).size()).isEqualTo(26);
    assertThat(out.get(minKeyB)).isNotNull();
    assertThat(out.get(minKeyB).size()).isEqualTo(12);
  }

  @Test
  public void testHistogramAccumulationWithBadLine() {
    in.add(ImmutableList.of(histoMinLineA, "not really valid...", histoMinLineA));

    histoSubject.run();
    cache.getResolveTask().run();

    assertThat(badPointsOut).hasSize(1);
    assertThat(badPointsOut.get(0)).contains("not really valid...");

    assertThat(out.get(minKeyA)).isNotNull();
    assertThat(out.get(minKeyA).size()).isEqualTo(26);
  }
}