package com.wavefront.agent.histogram;

import com.tdunning.math.stats.AgentDigest;
import com.wavefront.agent.PointHandler;

import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import sunnylabs.report.ReportPoint;

import static com.google.common.truth.Truth.assertThat;

/**
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class PointHandlerDispatcherTest {
  private final static short COMPRESSION = 100;

  private ConcurrentMap<Utils.HistogramKey, AgentDigest> in;
  private List<ReportPoint> pointOut;
  private List<String> debugLineOut;
  private List<String> blockedOut;
  private AtomicLong timeMillis;
  private PointHandlerDispatcher subject;

  private Utils.HistogramKey keyA = TestUtils.makeKey("keyA");
  private Utils.HistogramKey keyB = TestUtils.makeKey("keyB");
  private AgentDigest digestA;
  private AgentDigest digestB;


  @Before
  public void setup() {
    in = new ConcurrentHashMap<>();
    pointOut = new LinkedList<>();
    debugLineOut = new LinkedList<>();
    blockedOut = new LinkedList<>();
    digestA = new AgentDigest(COMPRESSION, 100L);
    digestB = new AgentDigest(COMPRESSION, 1000L);
    timeMillis = new AtomicLong(0L);
    subject = new PointHandlerDispatcher(in, new PointHandler() {

      @Override
      public void reportPoint(ReportPoint point, String debugLine) {
        pointOut.add(point);
        debugLineOut.add(debugLine);
      }

      @Override
      public void reportPoints(List<ReportPoint> points) {
        pointOut.addAll(points);
      }

      @Override
      public void handleBlockedPoint(String pointLine) {
        blockedOut.add(pointLine);
      }
    }, timeMillis::get);
  }

  @Test
  public void testBasicDispatch() {
    in.put(keyA, digestA);

    timeMillis.set(101L);
    subject.run();

    assertThat(pointOut).hasSize(1);
    assertThat(debugLineOut).hasSize(1);
    assertThat(blockedOut).hasSize(0);
    assertThat(in).isEmpty();

    ReportPoint point = pointOut.get(0);

    TestUtils.testKeyPointMatch(keyA, point);
  }

  @Test
  public void testOnlyRipeEntriesAreDispatched() {
    in.put(keyA, digestA);
    in.put(keyB, digestB);

    timeMillis.set(101L);
    subject.run();

    assertThat(pointOut).hasSize(1);
    assertThat(debugLineOut).hasSize(1);
    assertThat(blockedOut).hasSize(0);
    assertThat(in).containsEntry(keyB, digestB);

    ReportPoint point = pointOut.get(0);

    TestUtils.testKeyPointMatch(keyA, point);
  }
}