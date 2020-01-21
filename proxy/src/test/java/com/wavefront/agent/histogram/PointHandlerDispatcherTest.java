package com.wavefront.agent.histogram;

import com.tdunning.math.stats.AgentDigest;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.histogram.accumulator.AccumulationCache;

import com.wavefront.agent.histogram.accumulator.AgentDigestFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import wavefront.report.ReportPoint;

import static com.google.common.truth.Truth.assertThat;

/**
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class PointHandlerDispatcherTest {
  private final static short COMPRESSION = 100;

  private AccumulationCache in;
  private ConcurrentMap<HistogramKey, AgentDigest> backingStore;
  private List<ReportPoint> pointOut;
  private List<String> debugLineOut;
  private List<ReportPoint> blockedOut;
  private AtomicLong timeMillis;
  private PointHandlerDispatcher subject;

  private HistogramKey keyA = TestUtils.makeKey("keyA");
  private HistogramKey keyB = TestUtils.makeKey("keyB");
  private AgentDigest digestA;
  private AgentDigest digestB;


  @Before
  public void setup() {
    timeMillis = new AtomicLong(0L);
    backingStore = new ConcurrentHashMap<>();
    AgentDigestFactory agentDigestFactory = new AgentDigestFactory(() -> COMPRESSION, 100L,
        timeMillis::get);
    in = new AccumulationCache(backingStore, agentDigestFactory, 0, "", timeMillis::get);
    pointOut = new LinkedList<>();
    debugLineOut = new LinkedList<>();
    blockedOut = new LinkedList<>();
    digestA = new AgentDigest(COMPRESSION, 100L);
    digestB = new AgentDigest(COMPRESSION, 1000L);
    subject = new PointHandlerDispatcher(in, new ReportableEntityHandler<ReportPoint, String>() {

      @Override
      public void report(ReportPoint reportPoint) {
        pointOut.add(reportPoint);
      }

      @Override
      public void block(ReportPoint reportPoint) {
        blockedOut.add(reportPoint);
      }

      @Override
      public void block(@Nullable ReportPoint reportPoint, @Nullable String message) {
        blockedOut.add(reportPoint);
      }

      @Override
      public void reject(@Nullable ReportPoint reportPoint, @Nullable String message) {
        blockedOut.add(reportPoint);
      }

      @Override
      public void reject(@Nonnull String t, @Nullable String message) {
      }

      @Override
      public void shutdown() {
      }
    }, timeMillis::get, () -> false, null, null);
  }

  @Test
  public void testBasicDispatch() {
    in.put(keyA, digestA);
    in.flush();

    timeMillis.set(101L);
    subject.run();

    assertThat(pointOut).hasSize(1);
    assertThat(blockedOut).hasSize(0);
    assertThat(backingStore).isEmpty();

    ReportPoint point = pointOut.get(0);

    TestUtils.testKeyPointMatch(keyA, point);
  }

  @Test
  public void testOnlyRipeEntriesAreDispatched() {
    in.put(keyA, digestA);
    in.put(keyB, digestB);
    in.flush();

    timeMillis.set(101L);
    subject.run();
    in.flush();

    assertThat(pointOut).hasSize(1);
    assertThat(blockedOut).hasSize(0);
    assertThat(backingStore).containsEntry(keyB, digestB);

    ReportPoint point = pointOut.get(0);

    TestUtils.testKeyPointMatch(keyA, point);
  }
}
