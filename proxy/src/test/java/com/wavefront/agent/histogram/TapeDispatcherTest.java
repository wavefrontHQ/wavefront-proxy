package com.wavefront.agent.histogram;

import com.squareup.tape.InMemoryObjectQueue;
import com.squareup.tape.ObjectQueue;
import com.tdunning.math.stats.AgentDigest;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import wavefront.report.ReportPoint;

import static com.google.common.truth.Truth.assertThat;

/**
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class TapeDispatcherTest {
  private final static short COMPRESSION = 100;

  private ConcurrentMap<Utils.HistogramKey, AgentDigest> in;
  private ObjectQueue<ReportPoint> out;
  private AtomicLong timeMillis;
  private TapeDispatcher subject;

  private Utils.HistogramKey keyA = TestUtils.makeKey("keyA");
  private Utils.HistogramKey keyB = TestUtils.makeKey("keyB");
  private AgentDigest digestA;
  private AgentDigest digestB;


  @Before
  public void setup() {
    in = new ConcurrentHashMap<>();
    out = new InMemoryObjectQueue<>();
    digestA = new AgentDigest(COMPRESSION, 100L);
    digestB = new AgentDigest(COMPRESSION, 1000L);
    timeMillis = new AtomicLong(0L);
    subject = new TapeDispatcher(in, out, timeMillis::get);
  }

  @Test
  public void testBasicDispatch() {
    in.put(keyA, digestA);

    timeMillis.set(TimeUnit.MILLISECONDS.toNanos(101L));
    subject.run();

    assertThat(out.size()).isEqualTo(1);
    assertThat(in).isEmpty();

    ReportPoint point = out.peek();

    TestUtils.testKeyPointMatch(keyA, point);
  }

  @Test
  public void testOnlyRipeEntriesAreDispatched() {
    in.put(keyA, digestA);
    in.put(keyB, digestB);

    timeMillis.set(101L);
    subject.run();

    assertThat(out.size()).isEqualTo(1);
    assertThat(in).containsEntry(keyB, digestB);

    ReportPoint point = out.peek();

    TestUtils.testKeyPointMatch(keyA, point);
  }
}
