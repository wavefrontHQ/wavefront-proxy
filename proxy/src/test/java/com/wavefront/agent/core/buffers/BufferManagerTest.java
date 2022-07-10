package com.wavefront.agent.core.buffers;

import static com.wavefront.agent.TestUtils.assertTrueWithTimeout;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.wavefront.agent.TestQueue;
import com.wavefront.agent.TestUtils;
import com.wavefront.agent.core.queues.QueueInfo;
import com.yammer.metrics.core.Gauge;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.activemq.artemis.api.core.client.*;
import org.junit.Test;

public class BufferManagerTest {

  //  @After
  //  public void shutdown() {
  //    BuffersManager.shutdown();
  //  }

  @Test
  public void counters() throws Exception {
    Path buffer = Files.createTempDirectory("wfproxy");
    System.out.println("buffer: " + buffer);

    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.buffer = buffer.toFile().getAbsolutePath();
    cfg.msgExpirationTime = 10;
    cfg.l2 = true;
    BuffersManager.init(cfg);

    QueueInfo points = new TestQueue();
    BuffersManager.registerNewQueueIfNeedIt(points);

    for (int i = 0; i < 1_000_000; i++) {
      BuffersManager.sendMsg(points, "tururu");
    }
    BuffersManager.flush(points);

    for (int i = 0; i < 3; i++) {
      int pointsCount = 0;

      ServerLocator serverLocator = ActiveMQClient.createServerLocator("vm://" + 1);
      ClientSessionFactory factory = serverLocator.createSessionFactory();
      ClientSession session = factory.createSession(true, true);
      ClientConsumer client = session.createConsumer(points.getName() + ".0", true);

      long start = System.currentTimeMillis();
      boolean done = false;
      while (!done) {
        ClientMessage msg = client.receive(1000); // give time to the msg to move to disk
        if (msg != null) {
          pointsCount += msg.getIntProperty("points");
        } else {
          done = true;
        }
      }
      long time = System.currentTimeMillis() - start;
      assertEquals(1_000_000, pointsCount);

      System.out.println("-> pointsCount=" + pointsCount + " time:" + time);

      BuffersManager.init(cfg);
      BuffersManager.registerNewQueueIfNeedIt(points);
    }
  }

  @Test
  public void expiration() throws IOException, InterruptedException {
    Path buffer = Files.createTempDirectory("wfproxy");
    System.out.println("buffer: " + buffer);

    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.buffer = buffer.toFile().getAbsolutePath();
    cfg.l2 = true;
    cfg.msgExpirationTime = 100;
    cfg.msgRetry = -1;
    BuffersManager.init(cfg);

    QueueInfo points = new TestQueue();
    BuffersManager.registerNewQueueIfNeedIt(points);

    Gauge<Object> memory = BuffersManager.l1_getSizeGauge(points);
    Gauge<Object> disk = BuffersManager.l2_getSizeGauge(points);

    assertEquals("MessageCount", 0l, memory.value());
    BuffersManager.sendMsg(points, "tururu");
    BuffersManager.flush(points);
    assertNotEquals("MessageCount", 0l, memory.value());

    assertTrueWithTimeout(1000, () -> ((Long) memory.value()) != 0L);
    assertTrueWithTimeout(1000, () -> ((Long) disk.value()) != 0L);

    // the msg should not expire on disk queues
    Thread.sleep(1_000);
    assertNotEquals("MessageCount", 0l, disk.value());
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

    BuffersManager.registerNewQueueIfNeedIt(points);

    Gauge<Object> memory = BuffersManager.l1_getSizeGauge(points);
    Gauge<Object> disk = BuffersManager.l2_getSizeGauge(points);

    assertEquals("MessageCount", 0l, memory.value());
    BuffersManager.sendMsg(points, "tururu");
    BuffersManager.flush(points);
    assertNotEquals("MessageCount", 0l, memory.value());

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
    assertTrueWithTimeout(1000, () -> ((Long) memory.value()) == 0L);
    assertTrueWithTimeout(1000, () -> ((Long) disk.value()) != 0L);

    // the msg should not expire on disk queues
    Thread.sleep(1_000);
    assertNotEquals("MessageCount", 0l, disk.value());
  }

  @Test
  public void memoryQueueFull() throws IOException, InterruptedException {
    Path buffer = Files.createTempDirectory("wfproxy");
    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.l2 = true;
    cfg.msgRetry = -1;
    cfg.msgExpirationTime = -1;
    cfg.buffer = buffer.toFile().getAbsolutePath();
    BuffersManager.init(cfg);

    QueueInfo points = new TestQueue();
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
}
