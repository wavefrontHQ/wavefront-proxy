package com.wavefront.agent.histogram.accumulator;

import com.google.common.collect.Lists;

import com.squareup.tape.InMemoryObjectQueue;
import com.squareup.tape.ObjectQueue;
import com.tdunning.math.stats.AgentDigest;
import com.wavefront.agent.PointHandler;
import com.wavefront.agent.Validation;
import com.wavefront.agent.histogram.TestUtils;
import com.wavefront.agent.histogram.Utils;
import com.wavefront.agent.histogram.Utils.HistogramKey;
import com.wavefront.ingester.GraphiteDecoder;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import jersey.repackaged.com.google.common.collect.ImmutableList;
import sunnylabs.report.ReportPoint;

import static com.google.common.truth.Truth.assertThat;
import static com.wavefront.agent.histogram.TestUtils.DEFAULT_TIME_MILLIS;
import static com.wavefront.agent.histogram.TestUtils.DEFAULT_VALUE;
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
  private AccumulationTask subject;
  private final static long TTL = 30L;
  private final static  short COMPRESSION = 100;

  private String lineA = "keyA " + DEFAULT_VALUE + " " + DEFAULT_TIME_MILLIS;
  private String lineB = "keyB " + DEFAULT_VALUE + " " + DEFAULT_TIME_MILLIS;
  private String lineC = "keyC " + DEFAULT_VALUE + " " + DEFAULT_TIME_MILLIS;
  private HistogramKey keyA = TestUtils.makeKey("keyA");
  private HistogramKey keyB = TestUtils.makeKey("keyB");
  private HistogramKey keyC = TestUtils.makeKey("keyC");

  @Before
  public void setUp() throws Exception {
    in = new InMemoryObjectQueue<>();
    out = new ConcurrentHashMap<>();
    badPointsOut = Lists.newArrayList();

    subject = new AccumulationTask(
        in,
        out,
        new GraphiteDecoder("unknown", ImmutableList.of()),
        new PointHandler() {
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
        },
        Validation.Level.NUMERIC_ONLY,
        TTL,
        MINUTE,
        COMPRESSION);
  }

  @Test
  public void testSingleLine() {
    in.add(ImmutableList.of(lineA));

    subject.run();

    assertThat(out.get(keyA)).isNotNull();
    assertThat(out.get(keyA).size()).isEqualTo(1);
  }

  @Test
  public void testMultipleLines() {
    in.add(ImmutableList.of(lineA, lineB, lineC));

    subject.run();

    assertThat(out.get(keyA)).isNotNull();
    assertThat(out.get(keyA).size()).isEqualTo(1);
    assertThat(out.get(keyB)).isNotNull();
    assertThat(out.get(keyB).size()).isEqualTo(1);
    assertThat(out.get(keyC)).isNotNull();
    assertThat(out.get(keyC).size()).isEqualTo(1);
  }

  @Test
  public void testMultipleTimes() {
    in.add(ImmutableList.of(
        "min0 1 " + (DEFAULT_TIME_MILLIS - 60000),
        "min0 1 " + (DEFAULT_TIME_MILLIS - 60000),
        "min1 1 " + DEFAULT_TIME_MILLIS));

    subject.run();

    HistogramKey min0 = Utils.makeKey(ReportPoint.newBuilder()
        .setMetric("min0")
        .setTimestamp(DEFAULT_TIME_MILLIS - 60000)
        .setValue(1).build(), MINUTE);

    HistogramKey min1 = Utils.makeKey(ReportPoint.newBuilder()
        .setMetric("min1")
        .setTimestamp(DEFAULT_TIME_MILLIS)
        .setValue(1).build(), MINUTE);

    assertThat(out.get(min0)).isNotNull();
    assertThat(out.get(min0).size()).isEqualTo(2);
    assertThat(out.get(min1)).isNotNull();
    assertThat(out.get(min1).size()).isEqualTo(1);
  }

  @Test
  public void testAccumulation() {
    in.add(ImmutableList.of(lineA, lineA, lineA));

    subject.run();

    assertThat(out.get(keyA)).isNotNull();
    assertThat(out.get(keyA).size()).isEqualTo(3);
  }

  @Test
  public void testAccumulateWithBadLine() {
    in.add(ImmutableList.of("This is not really a valid sample", lineA, lineA, lineA));

    subject.run();

    assertThat(badPointsOut).hasSize(1);
    assertThat(badPointsOut.get(0)).contains("This is not really a valid sample");

    assertThat(out.get(keyA)).isNotNull();
    assertThat(out.get(keyA).size()).isEqualTo(3);
  }
}