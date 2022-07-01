package com.wavefront.agent.buffer;

import static com.wavefront.agent.TestUtils.assertTrueWithTimeout;
import static com.wavefront.data.ReportableEntityType.POINT;
import static org.junit.Assert.*;

import com.wavefront.agent.TestUtils;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.SenderTaskFactory;
import com.yammer.metrics.core.Gauge;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class BufferManagerTest {

  @Test
  public void expirationTest() throws IOException {
    Path buffer = Files.createTempDirectory("wfproxy");
    System.out.println("buffer: " + buffer);

    HandlerKey points = new HandlerKey(POINT, "2878");

    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.buffer = buffer.toFile().getAbsolutePath();
    cfg.msgExpirationTime = 500;
    cfg.msgRetry = -1;
    BuffersManager.init(cfg, senderTaskFactory, null);
    BuffersManager.registerNewQueueIfNeedIt(points);

    Gauge size_memory = BuffersManager.l1_getSizeGauge(points);
    Gauge size_disk = BuffersManager.l2_getSizeGauge(points);

    assertEquals("MessageCount", 0l, size_memory.value());
    assertEquals("MessageCount", 0l, size_disk.value());

    BuffersManager.sendMsg(points, "tururu");
    BuffersManager.flush(points);

    assertNotEquals("MessageCount", 0l, size_memory.value());
    assertEquals("MessageCount", 0l, size_disk.value());

    assertTrueWithTimeout(5000, () -> 0L == ((Long) size_memory.value()));
    assertTrueWithTimeout(5000, () -> 0L != ((Long) size_disk.value()));
  }

  @Test
  public void expiration_L2_Test() throws IOException, InterruptedException {
    Path buffer = Files.createTempDirectory("wfproxy");
    System.out.println("buffer: " + buffer);

    HandlerKey points = new HandlerKey(POINT, "2878");

    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.buffer = buffer.toFile().getAbsolutePath();
    cfg.l2 = true;
    cfg.msgExpirationTime = 100;
    cfg.msgRetry = -1;
    BuffersManager.init(cfg, senderTaskFactory, null);
    BuffersManager.registerNewQueueIfNeedIt(points);

    Gauge<Object> memory = BuffersManager.l1_getSizeGauge(points);
    Gauge<Object> disk = BuffersManager.l2_getSizeGauge(points);

    assertEquals("MessageCount", 0l, memory.value());
    BuffersManager.sendMsg(points, "tururu");
    assertEquals("MessageCount", 1l, memory.value());
    Thread.sleep(1_000);
    assertEquals("MessageCount", 0l, memory.value());
    assertEquals("MessageCount", 1l, disk.value());
    Thread.sleep(1_000);
    assertEquals("MessageCount", 1l, disk.value());
  }

  @Test
  public void MemoryQueueFull() throws IOException, InterruptedException {
    HandlerKey points = new HandlerKey(POINT, "2878");

    Path buffer = Files.createTempDirectory("wfproxy");
    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.l2 = true;
    cfg.msgRetry = -1;
    cfg.msgExpirationTime = -1;
    cfg.buffer = buffer.toFile().getAbsolutePath();
    BuffersManager.init(cfg, senderTaskFactory, null);

    BuffersManager.registerNewQueueIfNeedIt(points);

    // setting queue max size to 500 bytes
    BuffersManager.getLeve1().setQueueSize(points, 500);

    Gauge size_memory = BuffersManager.l1_getSizeGauge(points);
    Gauge size_disk = BuffersManager.l2_getSizeGauge(points);

    assertEquals("MessageCount", 0l, size_memory.value());
    assertEquals("MessageCount", 0l, size_disk.value());

    // 20 messages are around 619 bytes, that should go in the queue
    // and then mark the queue as full
    for (int i = 0; i < 20; i++) {
      BuffersManager.sendMsg(points, "tururu");
    }

    BuffersManager.flush(points);

    assertNotEquals("MessageCount", 0l, size_memory.value());
    assertEquals("MessageCount", 0l, size_disk.value());

    // the queue is already full, so this ones go directly to disk
    for (int i = 0; i < 20; i++) {
      BuffersManager.sendMsg(points, "tururu");
    }

    BuffersManager.flush(points);

    assertNotEquals("MessageCount", 0l, size_memory.value());
    assertNotEquals("MessageCount", 0l, size_disk.value());
  }

  @Test
  public void failDeliverTest() throws InterruptedException, IOException {
    Path buffer = Files.createTempDirectory("wfproxy");
    System.out.println("buffer: " + buffer);

    String msg = "tururu";

    HandlerKey points_2878 = new HandlerKey(POINT, "2878");

    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.buffer = buffer.toFile().getAbsolutePath();
    cfg.msgExpirationTime = -1;
    cfg.msgRetry = 3;
    BuffersManager.init(cfg, senderTaskFactory, null);
    BuffersManager.registerNewQueueIfNeedIt(points_2878);

    Gauge size_2878_memory = BuffersManager.l1_getSizeGauge(points_2878);
    Gauge size_2878_disk = BuffersManager.l2_getSizeGauge(points_2878);

    assertEquals("MessageCount", 0l, size_2878_memory.value());
    assertEquals("MessageCount", 0l, size_2878_disk.value());

    BuffersManager.sendMsg(points_2878, msg);

    assertEquals("MessageCount", 1l, size_2878_memory.value());
    assertEquals("MessageCount", 0l, size_2878_disk.value());

    // force MSG to DL
    for (int i = 0; i < 3; i++) {
      assertEquals("MessageCount", 1l, size_2878_memory.value());
      BuffersManager.onMsgBatch(
          points_2878,
          1,
          new TestUtils.RateLimiter(),
          msgs -> {
            assertEquals("MessageCount", msg, msgs);
            throw new Exception("error");
          });
    }

    Thread.sleep(1000); // wait some time to allow the msg to flight from l0 to l1

    assertEquals("MessageCount", 0l, size_2878_memory.value());
    assertEquals("MessageCount", 1l, size_2878_disk.value());
  }

  private static SenderTaskFactory senderTaskFactory =
      new SenderTaskFactory() {
        @Override
        public void createSenderTasks(@NotNull QueueInfo info, Buffer level_1) {}

        @Override
        public void shutdown() {}

        @Override
        public void shutdown(@NotNull String handle) {}

        @Override
        public void truncateBuffers() {}
      };
}
