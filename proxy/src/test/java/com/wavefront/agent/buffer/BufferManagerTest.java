package com.wavefront.agent.buffer;

import static com.wavefront.data.ReportableEntityType.POINT;
import static org.junit.Assert.assertEquals;

import com.wavefront.agent.handlers.HandlerKey;
import com.yammer.metrics.core.Gauge;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import org.junit.Test;

public class BufferManagerTest {

  @Test
  public void expirationTest() throws IOException, InterruptedException {
    Path buffer = Files.createTempDirectory("wfproxy");
    System.out.println("buffer: " + buffer);

    HandlerKey points = new HandlerKey(POINT, "2878");

    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.buffer = buffer.toFile().getAbsolutePath();
    BuffersManager.init(cfg);
    BuffersManager.registerNewHandlerKey(points);

    Gauge mc2878 = BuffersManager.l1GetMcGauge(points);
    assertEquals("MessageCount", 0l, mc2878.value());
    BuffersManager.sendMsg(points, Collections.singletonList("tururu"));
    assertEquals("MessageCount", 1l, mc2878.value());
    for (int i = 0; i < 60; i++) {
      assertEquals("MessageCount", 1l, mc2878.value());
      Thread.sleep(1_000);
    }
  }

  @Test
  public void expiration_L2_Test() throws IOException, InterruptedException {
    Path buffer = Files.createTempDirectory("wfproxy");
    System.out.println("buffer: " + buffer);

    HandlerKey points = new HandlerKey(POINT, "2878");

    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.buffer = buffer.toFile().getAbsolutePath();
    cfg.l2 = true;
    BuffersManager.init(cfg);
    BuffersManager.registerNewHandlerKey(points);

    Gauge<Long> memory = BuffersManager.l1GetMcGauge(points);
    Gauge<Long> disk = BuffersManager.l2GetMcGauge(points);

    assertEquals("MessageCount", 0l, memory.value().longValue());
    BuffersManager.sendMsg(points, Collections.singletonList("tururu"));
    assertEquals("MessageCount", 1l, memory.value().longValue());
    for (int i = 0; i < 10; i++) {
      if (memory.value() != 1) {
        break;
      }
      Thread.sleep(1_000);
    }
    Thread.sleep(1_000);
    assertEquals("MessageCount", 0l, memory.value().longValue());
    assertEquals("MessageCount", 1l, disk.value().longValue());

    for (int i = 0; i < 10; i++) {
      if (disk.value() != 1) {
        break;
      }
      Thread.sleep(1_000);
    }
    assertEquals("MessageCount", 1l, disk.value().longValue());

    BuffersManager.getLeve2().onMsg(points, msg -> assertEquals("MessageCount", "tururu2", msg));
  }

  @Test
  public void initTest() throws InterruptedException, IOException {
    Path buffer = Files.createTempDirectory("wfproxy");
    System.out.println("buffer: " + buffer);

    HandlerKey points_2878 = new HandlerKey(POINT, "2878");
    HandlerKey points_2879 = new HandlerKey(POINT, "2879");

    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.buffer = buffer.toFile().getAbsolutePath();
    BuffersManager.init(cfg);
    BuffersManager.registerNewHandlerKey(points_2878);
    BuffersManager.registerNewHandlerKey(points_2879);

    Gauge mc2878_memory = BuffersManager.l1GetMcGauge(points_2878);
    Gauge mc2878_disk = BuffersManager.l2GetMcGauge(points_2878);
    Gauge mc2879 = BuffersManager.l1GetMcGauge(points_2879);

    assertEquals("MessageCount", 0l, mc2878_memory.value());
    assertEquals("MessageCount", 0l, mc2878_disk.value());
    assertEquals("MessageCount", 0l, mc2879.value());

    BuffersManager.sendMsg(points_2878, Collections.singletonList("tururu"));
    BuffersManager.sendMsg(points_2879, Collections.singletonList("tururu2"));

    assertEquals("MessageCount", 1l, mc2878_memory.value());
    assertEquals("MessageCount", 0l, mc2878_disk.value());
    assertEquals("MessageCount", 1l, mc2879.value());

    // force MSG to DL
    for (int i = 0; i < 3; i++) {
      assertEquals("MessageCount", 1l, mc2878_memory.value());
      BuffersManager.onMsg(
          points_2878,
          msg -> {
            assertEquals("MessageCount", "tururu", msg);
            throw new Exception("error");
          });
    }

    Thread.sleep(1000); // wait some time to allow the msg to flight from l0 to l1

    assertEquals("MessageCount", 0l, mc2878_memory.value());
    assertEquals("MessageCount", 1l, mc2878_disk.value());
    assertEquals("MessageCount", 1l, mc2879.value());

    BuffersManager.onMsg(points_2879, msg -> assertEquals("MessageCount", "tururu2", msg));

    assertEquals("MessageCount", 0l, mc2878_memory.value());
    assertEquals("MessageCount", 1l, mc2878_disk.value());
    assertEquals("MessageCount", 0l, mc2879.value());

    Thread.sleep(60000); // wait some time to allow the msg to flight from l0 to l1
    assertEquals("MessageCount", 0l, mc2878_memory.value());
    assertEquals("MessageCount", 1l, mc2878_disk.value());
    assertEquals("MessageCount", 0l, mc2879.value());
  }
}
