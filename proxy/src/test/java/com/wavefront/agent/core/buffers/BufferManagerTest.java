package com.wavefront.agent.core.buffers;

import static com.wavefront.agent.TestUtils.assertTrueWithTimeout;
import static org.junit.Assert.*;

import com.wavefront.agent.TestUtils;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.core.queues.QueueStats;
import com.wavefront.agent.core.queues.TestQueue;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.Metrics;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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

    sqs.onMsgBatch(points, 0, new FailCallBack());

    sqs.sendPoints(points.getName(), Collections.singletonList("tururu"));

    Thread.sleep(2000); // wait until the failed message get visible again

    AtomicBoolean done = new AtomicBoolean(true);
    sqs.onMsgBatch(
        points,
        0,
        new NoLimitCallBack() {
          @Override
          public void processBatch(List<String> batch) throws Exception {
            assertEquals(1, batch.size());
            assertEquals("tururu", batch.get(0));
            done.set(true);
          }
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

    memory.onMsgBatch(points, 0, new FailCallBack());
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
            new NoLimitCallBack() {
              @Override
              public void processBatch(List<String> batch) throws Exception {
                ok.set(batch.get(0).equals("tururu"));
              }
            });
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
      BuffersManager.onMsgBatch(points, 0, new TestUtils.RateLimiter(), new FailCallBack());
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
    cfg.memoryCfg.maxMemory = 2000;
    BuffersManager.init(cfg);

    QueueInfo points = new TestQueue(ReportableEntityType.POINT);
    List<Buffer> buffers = BuffersManager.registerNewQueueIfNeedIt(points);
    MemoryBuffer memory = (MemoryBuffer) buffers.get(0);
    DiskBuffer disk = (DiskBuffer) buffers.get(1);

    assertEquals("MessageCount", 0, memory.countMetrics.get(points.getName()).doCount());
    assertEquals("MessageCount", 0, disk.countMetrics.get(points.getName()).doCount());

    for (int i = 0; i < 100; i++) {
      BuffersManager.sendMsg(points, "tururu");
    }

    memory.flush(points);
    Thread.sleep(1_000);

    assertNotEquals("MessageCount", 0, memory.countMetrics.get(points.getName()).doCount());
    assertNotEquals("MessageCount", 0, disk.countMetrics.get(points.getName()).doCount());

    // the queue is already full, so this ones go directly to disk
    for (int i = 0; i < 20; i++) {
      BuffersManager.sendMsg(points, "tururu");
    }
  }

  @Test
  public void exporter() throws IOException, InterruptedException {
    Path buffer = Files.createTempDirectory("wfproxy");
    int nMsgs = 100_000;
    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.disk = true;
    cfg.diskCfg.buffer = buffer.toFile();
    TestQueue points = new TestQueue(5, ReportableEntityType.POINT, false);
    points.itemsPM = 100;
    TestQueue logs = new TestQueue(5, ReportableEntityType.LOGS, false);

    BuffersManager.init(cfg);
    BuffersManager.registerNewQueueIfNeedIt(logs);
    List<Buffer> buffers = BuffersManager.registerNewQueueIfNeedIt(points);
    MemoryBuffer memory = (MemoryBuffer) buffers.get(0);
    DiskBuffer disk = (DiskBuffer) buffers.get(1);

    for (int i = 0; i < 10; i++) {
      BuffersManager.sendMsg(logs, "tururu");
    }
    for (int i = 0; i < nMsgs; i++) {
      BuffersManager.sendMsg(points, "tururu");
    }
    memory.flush(points);
    Thread.sleep(1000);
    assertEquals("MessageCount", nMsgs, memory.countMetrics.get(points.getName()).doCount());
    assertEquals("MessageCount", 0, disk.countMetrics.get(points.getName()).doCount());

    BuffersManager.shutdown();

    Path exportPath = Files.createTempDirectory("export");

    // Export RetainData = true
    Exporter.export(buffer.toString(), exportPath.toFile().getAbsolutePath(), "points,logs", true);
    int c = 0;
    try (BufferedReader reader =
        new BufferedReader(new FileReader(new File(exportPath.toFile(), "points.txt")))) {
      String line = reader.readLine();
      while (line != null) {
        c++;
        assertEquals("tururu", line);
        line = reader.readLine();
      }
    }
    assertEquals(nMsgs, c);

    c = 0;
    try (BufferedReader reader =
        new BufferedReader(new FileReader(new File(exportPath.toFile(), "logs.txt")))) {
      String line = reader.readLine();
      while (line != null) {
        c++;
        assertEquals("tururu", line);
        line = reader.readLine();
      }
    }
    assertEquals(10, c);

    // Export RetainData = false
    Exporter.export(buffer.toString(), exportPath.toFile().getAbsolutePath(), "points", false);
    c = 0;
    try (BufferedReader reader =
        new BufferedReader(new FileReader(new File(exportPath.toFile(), "points.txt")))) {
      String line = reader.readLine();
      while (line != null) {
        c++;
        assertEquals("tururu", line);
        line = reader.readLine();
      }
    }
    assertEquals(nMsgs, c);

    // Export but the buffer is empty
    Exporter.export(buffer.toString(), exportPath.toFile().getAbsolutePath(), "points", true);
    c = 0;
    try (BufferedReader reader =
        new BufferedReader(new FileReader(new File(exportPath.toFile(), "points.txt")))) {
      String line = reader.readLine();
      while (line != null) {
        c++;
        assertEquals("tururu", line);
        line = reader.readLine();
      }
    }
    assertEquals(0, c);
  }

  @Test
  public void checkBatchSize() {
    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.disk = false;
    BuffersManager.init(cfg);

    QueueInfo points = new TestQueue(1, ReportableEntityType.POINT);
    MemoryBuffer memory = (MemoryBuffer) BuffersManager.registerNewQueueIfNeedIt(points).get(0);

    for (int i = 0; i < 4_321; i++) {
      BuffersManager.sendMsg(points, "tururu");
    }
    memory.flush(points);

    final boolean[] error = {false};

    while (memory.countMetrics.get(points.getName()).doCount() != 0) {
      memory.onMsgBatch(
          points,
          0,
          new OnMsgDelegate() {
            @Override
            public void processBatch(List<String> batch) throws Exception {
              System.out.println("Pay Load = " + batch.size());
              error[0] = batch.size() > 250;
              assertFalse("Pay Load size (" + batch.size() + ") overflow", error[0]);
            }

            @Override
            public boolean checkBatchSize(int items, int bytes, int newItems, int newBytes) {
              return items + newItems <= 250;
            }

            @Override
            public boolean checkRates(int newItems, int newBytes) {
              return true;
            }
          });
      assertFalse(error[0]);
    }
  }

  @Test
  public void checkRates() {
    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.disk = false;
    BuffersManager.init(cfg);

    QueueInfo points = new TestQueue(1, ReportableEntityType.POINT);
    MemoryBuffer memory = (MemoryBuffer) BuffersManager.registerNewQueueIfNeedIt(points).get(0);

    for (int i = 0; i < 4_321; i++) {
      BuffersManager.sendMsg(points, "tururu");
    }
    memory.flush(points);

    final boolean[] error = {false};

    while (memory.countMetrics.get(points.getName()).doCount() != 0) {
      memory.onMsgBatch(
          points,
          0,
          new OnMsgDelegate() {
            int rate = 0;

            @Override
            public void processBatch(List<String> batch) throws Exception {
              System.out.println("Pay Load = " + batch.size());
              error[0] = batch.size() > 250;
              assertFalse("Pay Load size (" + batch.size() + ") overflow", error[0]);
            }

            @Override
            public boolean checkBatchSize(int items, int bytes, int newItems, int newBytes) {
              return true;
            }

            @Override
            public boolean checkRates(int newItems, int newBytes) {
              rate += newItems;
              return rate < 250;
            }
          });
      assertFalse(error[0]);
    }
  }

  private abstract class NoLimitCallBack implements OnMsgDelegate {
    @Override
    public boolean checkBatchSize(int items, int bytes, int newItems, int newBytes) {
      return true;
    }

    @Override
    public boolean checkRates(int newItems, int newBytes) {
      return true;
    }
  }

  private class FailCallBack extends NoLimitCallBack {
    @Override
    public void processBatch(List<String> batch) throws Exception {
      throw new RuntimeException("force fail");
    }
  }
}
