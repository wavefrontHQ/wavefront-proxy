package com.wavefront.agent.core.buffers;

import static com.wavefront.agent.TestUtils.assertTrueWithTimeout;
import static org.junit.Assert.assertEquals;

import com.wavefront.agent.TestUtils;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.agent.core.queues.TestQueue;
import com.yammer.metrics.Metrics;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.activemq.artemis.api.core.ActiveMQAddressFullException;
import org.junit.After;
import org.junit.Test;

public class BufferManagerTest {

  @After
  public void teardown() {
    System.out.println("Test done");
    BuffersManager.shutdown();
  }

  @Test
  public void shutdown() throws Exception {
    Path buffer = Files.createTempDirectory("wfproxy");
    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.buffer = buffer.toFile().getAbsolutePath();
    cfg.msgExpirationTime = -1;
    cfg.l2 = true;
    BuffersManager.init(cfg);

    QueueInfo points = new TestQueue();
    List<Buffer> buffers = BuffersManager.registerNewQueueIfNeedIt(points);
    MemoryBuffer memory = (MemoryBuffer) buffers.get(0);
    DiskBuffer disk = (DiskBuffer) buffers.get(1);

    for (int i = 0; i < 10_000; i++) {
      BuffersManager.sendMsg(points, "tururu");
    }
    memory.flush(points);

    assertEquals("MessageCount", 10_000, memory.countMetrics.get(points.getName()).doCount());
    assertEquals("MessageCount", 0, disk.countMetrics.get(points.getName()).doCount());

    BuffersManager.shutdown();

    // we need to delete all metrics so counters gets regenerated.
    Metrics.defaultRegistry()
        .allMetrics()
        .keySet()
        .forEach(metricName -> Metrics.defaultRegistry().removeMetric(metricName));

    BuffersManager.init(cfg);
    buffers = BuffersManager.registerNewQueueIfNeedIt(points);
    memory = (MemoryBuffer) buffers.get(0);
    disk = (DiskBuffer) buffers.get(1);

    assertEquals("MessageCount", 10_000, disk.countMetrics.get(points.getName()).doCount());
    assertEquals("MessageCount", 0, memory.countMetrics.get(points.getName()).doCount());
  }

  @Test
  public void counters() {
    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.l2 = false;
    BuffersManager.init(cfg);

    QueueInfo points = new TestQueue(8);
    MemoryBuffer memory = (MemoryBuffer) BuffersManager.registerNewQueueIfNeedIt(points).get(0);

    for (int i = 0; i < 1_654_321; i++) {
      BuffersManager.sendMsg(points, "tururu");
    }
    memory.flush(points);
    assertEquals("gauge.doCount", 1_654_321, memory.countMetrics.get(points.getName()).doCount());
  }

  @Test
  public void bridgeControl() throws IOException, InterruptedException {
    Path buffer = Files.createTempDirectory("wfproxy");
    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.buffer = buffer.toFile().getAbsolutePath();
    cfg.l2 = true;
    cfg.msgExpirationTime = -1;
    cfg.msgRetry = 1;
    BuffersManager.init(cfg);

    QueueInfo points = new TestQueue();
    List<Buffer> buffers = BuffersManager.registerNewQueueIfNeedIt(points);
    MemoryBuffer memory = (MemoryBuffer) buffers.get(0);
    DiskBuffer disk = (DiskBuffer) buffers.get(1);

    send100pointsAndFail(points, memory);

    assertEquals("failed", 100, QueueStats.get(points.getName()).queuedFailed.count());
    assertEquals("failed", 0, QueueStats.get(points.getName()).queuedExpired.count());
    assertEquals("failed", 100, disk.countMetrics.get(points.getName()).doCount());
    assertEquals("failed", 0, memory.countMetrics.get(points.getName()).doCount());

    memory.disableBridge();

    send100pointsAndFail(points, memory);

    assertEquals("failed", 100, QueueStats.get(points.getName()).queuedFailed.count());
    assertEquals("failed", 0, QueueStats.get(points.getName()).queuedExpired.count());
    assertEquals("failed", 100, disk.countMetrics.get(points.getName()).doCount());
    assertEquals("failed", 100, memory.countMetrics.get(points.getName()).doCount());
  }

  private void send100pointsAndFail(QueueInfo points, MemoryBuffer memory) {
    for (int i = 0; i < 100; i++) {
      BuffersManager.sendMsg(points, "tururu");
    }
    memory.flush(points);

    memory.onMsgBatch(
        points,
        0,
        1000,
        new TestUtils.RateLimiter(),
        batch -> {
          throw new RuntimeException("force fail");
        });
  }

  @Test
  public void expiration() throws IOException, InterruptedException, ActiveMQAddressFullException {
    Path buffer = Files.createTempDirectory("wfproxy");
    System.out.println("buffer: " + buffer);

    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.buffer = buffer.toFile().getAbsolutePath();
    cfg.l2 = true;
    cfg.msgExpirationTime = 100;
    cfg.msgRetry = -1;
    BuffersManager.init(cfg);

    QueueInfo points = new TestQueue();
    List<Buffer> buffers = BuffersManager.registerNewQueueIfNeedIt(points);
    MemoryBuffer memory = (MemoryBuffer) buffers.get(0);
    DiskBuffer disk = (DiskBuffer) buffers.get(1);

    assertEquals("MessageCount", 0, memory.countMetrics.get(points.getName()).doCount());
    BuffersManager.sendMsg(points, "tururu");
    memory.flush(points);
    assertEquals("MessageCount", 1, memory.countMetrics.get(points.getName()).doCount());

    assertTrueWithTimeout(1000, () -> memory.countMetrics.get(points.getName()).doCount() == 0);
    assertTrueWithTimeout(1000, () -> disk.countMetrics.get(points.getName()).doCount() == 1);

    // the msg should not expire on disk queues
    Thread.sleep(1_000);
    assertEquals("MessageCount", 1, disk.countMetrics.get(points.getName()).doCount());

    AtomicBoolean ok = new AtomicBoolean(false);
    buffers
        .get(1)
        .onMsgBatch(
            points,
            0,
            1000,
            new TestUtils.RateLimiter(),
            batch -> {
              ok.set(batch.get(0).equals("tururu"));
            });
    assertTrueWithTimeout(3000, () -> ok.get());

    assertEquals("queuedFailed", 0, QueueStats.get(points.getName()).queuedFailed.count());
    assertEquals("queuedExpired", 1, QueueStats.get(points.getName()).queuedExpired.count());
  }

  @Test
  public void fail() throws IOException, InterruptedException {
    Path buffer = Files.createTempDirectory("wfproxy");
    System.out.println("buffer: " + buffer);

    QueueInfo points = new TestQueue();

    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.buffer = buffer.toFile().getAbsolutePath();
    cfg.l2 = true;
    cfg.msgExpirationTime = -1;
    cfg.msgRetry = 2;
    BuffersManager.init(cfg);

    List<Buffer> buffers = BuffersManager.registerNewQueueIfNeedIt(points);
    MemoryBuffer memory = (MemoryBuffer) buffers.get(0);
    DiskBuffer disk = (DiskBuffer) buffers.get(1);

    assertEquals("queuedFailed", 0, QueueStats.get(points.getName()).queuedFailed.count());
    assertEquals("queuedExpired", 0, QueueStats.get(points.getName()).queuedExpired.count());

    assertEquals("MessageCount", 0, memory.countMetrics.get(points.getName()).doCount());
    BuffersManager.sendMsg(points, "tururu");
    memory.flush(points);
    assertEquals("MessageCount", 1, memory.countMetrics.get(points.getName()).doCount());

    for (int i = 0; i < 4; i++) {
      BuffersManager.onMsgBatch(
          points,
          0,
          1000,
          new TestUtils.RateLimiter(),
          batch -> {
            throw new RuntimeException("error 500");
          });
    }
    assertTrueWithTimeout(1000, () -> memory.countMetrics.get(points.getName()).doCount() == 0);
    assertTrueWithTimeout(1000, () -> disk.countMetrics.get(points.getName()).doCount() == 1);

    // the msg should not expire on disk queues
    Thread.sleep(1_000);
    assertEquals("MessageCount", 1, disk.countMetrics.get(points.getName()).doCount());

    assertEquals("queuedFailed", 1, QueueStats.get(points.getName()).queuedFailed.count());
    assertEquals("queuedExpired", 0, QueueStats.get(points.getName()).queuedExpired.count());
  }

  @Test
  public void memoryQueueFull() throws IOException, InterruptedException {
    Path buffer = Files.createTempDirectory("wfproxy");
    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.l2 = true;
    cfg.msgRetry = -1;
    cfg.msgExpirationTime = -1;
    cfg.buffer = buffer.toFile().getAbsolutePath();
    cfg.memoryMaxMemory = 500;
    BuffersManager.init(cfg);

    QueueInfo points = new TestQueue();
    List<Buffer> buffers = BuffersManager.registerNewQueueIfNeedIt(points);
    MemoryBuffer memory = (MemoryBuffer) buffers.get(0);
    DiskBuffer disk = (DiskBuffer) buffers.get(1);

    assertEquals("MessageCount", 0l, memory.countMetrics.get(points.getName()).doCount());
    assertEquals("MessageCount", 0l, disk.countMetrics.get(points.getName()).doCount());

    // 20 messages are around 619 bytes, that should go in the queue
    // and then mark the queue as full
    for (int i = 0; i < 20; i++) {
      BuffersManager.sendMsg(points, "tururu");
    }

    memory.flush(points);

    assertEquals("MessageCount", 20l, memory.countMetrics.get(points.getName()).doCount());
    assertEquals("MessageCount", 0l, disk.countMetrics.get(points.getName()).doCount());

    // the queue is already full, so this ones go directly to disk
    for (int i = 0; i < 20; i++) {
      BuffersManager.sendMsg(points, "tururu");
    }

    memory.flush(points);

    assertEquals("MessageCount", 20l, memory.countMetrics.get(points.getName()).doCount());
    assertEquals("MessageCount", 20l, disk.countMetrics.get(points.getName()).doCount());
  }
}
