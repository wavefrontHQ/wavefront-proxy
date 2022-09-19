package com.wavefront.agent.core.buffers;

import static com.wavefront.agent.TestUtils.assertTrueWithTimeout;
import static org.junit.Assert.assertEquals;

import com.wavefront.agent.TestUtils;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.agent.core.queues.TestQueue;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

public class BufferManagerTest {

  @After
  public void teardown() {
    System.out.println("Test done");
    BuffersManager.shutdown();
  }

  @Test
  @Ignore // need external resources that not always is available we will write a functional test
  // for this
  public void external() throws Exception {
    SQSBufferConfig sqsCfg = new SQSBufferConfig();
    sqsCfg.template = "wf-proxy-{{id}}-{{entity}}-{{port}}";
    sqsCfg.region = "us-west-2";
    sqsCfg.vto = 1;

    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.disk = false;
    cfg.external = true;
    cfg.sqsCfg = sqsCfg;
    BuffersManager.init(cfg);

    QueueInfo points = new TestQueue(ReportableEntityType.POINT);
    List<Buffer> buffers = BuffersManager.registerNewQueueIfNeedIt(points);
    SQSBuffer sqs = (SQSBuffer) buffers.get(1);

    // just in case
    sqs.truncateQueue(points.getName());

    sqs.sendPoints(points.getName(), Collections.singletonList("tururu"));

    sqs.onMsgBatch(
        points,
        0,
        100,
        new TestUtils.RateLimiter(),
        batch -> {
          throw new RuntimeException("force fail");
        });

    sqs.sendPoints(points.getName(), Collections.singletonList("tururu"));

    Thread.sleep(2000); // wait until the failed message get visible again

    AtomicBoolean done = new AtomicBoolean(true);
    sqs.onMsgBatch(
        points,
        0,
        100,
        new TestUtils.RateLimiter(),
        batch -> {
          assertEquals(1, batch.size());
          assertEquals("tururu", batch.get(0));
          done.set(true);
        });
    assertTrueWithTimeout(10000, done::get);
  }

  @Test
  public void shutdown() throws Exception {
    Path buffer = Files.createTempDirectory("wfproxy");
    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.disk = true;
    cfg.diskCfg.buffer = buffer.toFile();
    cfg.memoryCfg.msgExpirationTime = -1;
    BuffersManager.init(cfg);

    QueueInfo points = new TestQueue(ReportableEntityType.POINT);
    List<Buffer> buffers = BuffersManager.registerNewQueueIfNeedIt(points);
    MemoryBuffer memory = (MemoryBuffer) buffers.get(0);
    DiskBuffer disk = (DiskBuffer) buffers.get(1);

    for (int i = 0; i < 10_000; i++) {
      BuffersManager.sendMsg(points, "tururu");
    }
    memory.flush(points);
    Thread.sleep(1_000);

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
  public void counters() throws InterruptedException {
    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.disk = false;
    BuffersManager.init(cfg);

    QueueInfo points = new TestQueue(8, ReportableEntityType.POINT);
    MemoryBuffer memory = (MemoryBuffer) BuffersManager.registerNewQueueIfNeedIt(points).get(0);

    for (int i = 0; i < 1_654_321; i++) {
      BuffersManager.sendMsg(points, "tururu");
    }
    memory.flush(points);
    Thread.sleep(1_000);
    assertEquals("gauge.doCount", 1_654_321, memory.countMetrics.get(points.getName()).doCount());
  }

  @Test
  public void bridgeControl() throws IOException, InterruptedException {
    Path buffer = Files.createTempDirectory("wfproxy");
    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.disk = true;
    cfg.diskCfg.buffer = buffer.toFile();
    cfg.memoryCfg.msgExpirationTime = -1;
    cfg.memoryCfg.msgRetry = 1;
    BuffersManager.init(cfg);

    QueueInfo points = new TestQueue(ReportableEntityType.POINT);
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

  private void send100pointsAndFail(QueueInfo points, MemoryBuffer memory)
      throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      BuffersManager.sendMsg(points, "tururu");
    }
    memory.flush(points);
    Thread.sleep(1_000);

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
  public void expiration() throws IOException, InterruptedException {
    Path buffer = Files.createTempDirectory("wfproxy");
    System.out.println("buffer: " + buffer);

    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.disk = true;
    cfg.diskCfg.buffer = buffer.toFile();
    cfg.memoryCfg.msgExpirationTime = 100;
    cfg.memoryCfg.msgRetry = -1;
    BuffersManager.init(cfg);

    QueueInfo points = new TestQueue(ReportableEntityType.POINT);
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
            batch -> ok.set(batch.get(0).equals("tururu")));
    assertTrueWithTimeout(3000, ok::get);

    assertEquals("queuedFailed", 0, QueueStats.get(points.getName()).queuedFailed.count());
    assertEquals("queuedExpired", 1, QueueStats.get(points.getName()).queuedExpired.count());
  }

  @Test
  public void fail() throws IOException, InterruptedException {
    Path buffer = Files.createTempDirectory("wfproxy");
    System.out.println("buffer: " + buffer);

    QueueInfo points = new TestQueue(ReportableEntityType.POINT);

    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.disk = true;
    cfg.diskCfg.buffer = buffer.toFile();
    cfg.memoryCfg.msgExpirationTime = -1;
    cfg.memoryCfg.msgRetry = 2;
    BuffersManager.init(cfg);

    List<Buffer> buffers = BuffersManager.registerNewQueueIfNeedIt(points);
    MemoryBuffer memory = (MemoryBuffer) buffers.get(0);
    DiskBuffer disk = (DiskBuffer) buffers.get(1);

    assertEquals("queuedFailed", 0, QueueStats.get(points.getName()).queuedFailed.count());
    assertEquals("queuedExpired", 0, QueueStats.get(points.getName()).queuedExpired.count());

    assertEquals("MessageCount", 0, memory.countMetrics.get(points.getName()).doCount());
    BuffersManager.sendMsg(points, "tururu");
    memory.flush(points);
    Thread.sleep(1_000);
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
    cfg.disk = true;
    cfg.diskCfg.buffer = buffer.toFile();
    cfg.memoryCfg.msgRetry = -1;
    cfg.memoryCfg.msgExpirationTime = -1;
    cfg.memoryCfg.maxMemory = 500;
    BuffersManager.init(cfg);

    QueueInfo points = new TestQueue(ReportableEntityType.POINT);
    List<Buffer> buffers = BuffersManager.registerNewQueueIfNeedIt(points);
    MemoryBuffer memory = (MemoryBuffer) buffers.get(0);
    DiskBuffer disk = (DiskBuffer) buffers.get(1);

    assertEquals("MessageCount", 0, memory.countMetrics.get(points.getName()).doCount());
    assertEquals("MessageCount", 0, disk.countMetrics.get(points.getName()).doCount());

    // 20 messages are around 619 bytes, that should go in the queue
    // and then mark the queue as full
    for (int i = 0; i < 20; i++) {
      BuffersManager.sendMsg(points, "tururu");
    }

    memory.flush(points);
    Thread.sleep(1_000);

    assertEquals("MessageCount", 20, memory.countMetrics.get(points.getName()).doCount());
    assertEquals("MessageCount", 0, disk.countMetrics.get(points.getName()).doCount());

    // the queue is already full, so this ones go directly to disk
    for (int i = 0; i < 20; i++) {
      BuffersManager.sendMsg(points, "tururu");
    }

    memory.flush(points);
    Thread.sleep(1_000);

    assertEquals("MessageCount", 20, memory.countMetrics.get(points.getName()).doCount());
    assertEquals("MessageCount", 20, disk.countMetrics.get(points.getName()).doCount());
  }
}
