package com.wavefront.agent.buffer;

import static org.junit.Assert.assertEquals;

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

    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.buffer = buffer.toFile().getAbsolutePath();
    BuffersManager.init(cfg);
    BuffersManager.registerNewPort("2878");

    Gauge mc2878 = BuffersManager.l1GetMcGauge("2878");
    assertEquals("MessageCount", 0, mc2878.value());
    BuffersManager.sendMsg("2878", Collections.singletonList("tururu"));
    assertEquals("MessageCount", 1, mc2878.value());
    for (int i = 0; i < 60; i++) {
      assertEquals("MessageCount", 1, mc2878.value());
      Thread.sleep(1_000);
    }
  }

  @Test
  public void expiration_L2_Test() throws IOException, InterruptedException {
    Path buffer = Files.createTempDirectory("wfproxy");
    System.out.println("buffer: " + buffer);

    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.buffer = buffer.toFile().getAbsolutePath();
    cfg.l2 = true;
    BuffersManager.init(cfg);
    BuffersManager.registerNewPort("2878");

    Gauge<Long> memory = BuffersManager.l1GetMcGauge("2878");
    Gauge<Long> disk = BuffersManager.l2GetMcGauge("2878");

    assertEquals("MessageCount", 0l, memory.value().longValue());
    BuffersManager.sendMsg("2878", Collections.singletonList("tururu"));
    assertEquals("MessageCount", 1, memory.value().longValue());
    for (int i = 0; i < 10; i++) {
      if (memory.value() != 1) {
        break;
      }
      Thread.sleep(1_000);
    }
    Thread.sleep(1_000);
    assertEquals("MessageCount", 0, memory.value().longValue());
    assertEquals("MessageCount", 1, disk.value().longValue());

    for (int i = 0; i < 10; i++) {
      if (disk.value() != 1) {
        break;
      }
      Thread.sleep(1_000);
    }
    assertEquals("MessageCount", 1, disk.value().longValue());

    BuffersManager.getLeve2().onMsg("2879", msg -> assertEquals("MessageCount", "tururu2", msg));
  }

  @Test
  public void initTest() throws InterruptedException, IOException {
    Path buffer = Files.createTempDirectory("wfproxy");
    System.out.println("buffer: " + buffer);

    BuffersManagerConfig cfg = new BuffersManagerConfig();
    cfg.buffer = buffer.toFile().getAbsolutePath();
    BuffersManager.init(cfg);
    BuffersManager.registerNewPort("2878");
    BuffersManager.registerNewPort("2879");

    Gauge mc2878_memory = BuffersManager.l1GetMcGauge("2878");
    Gauge mc2878_disk = BuffersManager.l2GetMcGauge("2878");
    Gauge mc2879 = BuffersManager.l1GetMcGauge("2879");

    assertEquals("MessageCount", 0, mc2878_memory.value());
    assertEquals("MessageCount", 0, mc2878_disk.value());
    assertEquals("MessageCount", 0, mc2879.value());

    BuffersManager.sendMsg("2878", Collections.singletonList("tururu"));
    BuffersManager.sendMsg("2879", Collections.singletonList("tururu2"));

    assertEquals("MessageCount", 1, mc2878_memory.value());
    assertEquals("MessageCount", 0, mc2878_disk.value());
    assertEquals("MessageCount", 1, mc2879.value());

    // force MSG to DL
    for (int i = 0; i < 3; i++) {
      assertEquals("MessageCount", 1, mc2878_memory.value());
      BuffersManager.onMsg(
          "2878",
          msg -> {
            assertEquals("MessageCount", "tururu", msg);
            throw new Exception("error");
          });
    }

    Thread.sleep(1000); // wait some time to allow the msg to flight from l0 to l1

    assertEquals("MessageCount", 0, mc2878_memory.value());
    assertEquals("MessageCount", 1, mc2878_disk.value());
    assertEquals("MessageCount", 1, mc2879.value());

    BuffersManager.onMsg("2879", msg -> assertEquals("MessageCount", "tururu2", msg));

    assertEquals("MessageCount", 0, mc2878_memory.value());
    assertEquals("MessageCount", 1, mc2878_disk.value());
    assertEquals("MessageCount", 0, mc2879.value());

    Thread.sleep(60000); // wait some time to allow the msg to flight from l0 to l1
    assertEquals("MessageCount", 0, mc2878_memory.value());
    assertEquals("MessageCount", 1, mc2878_disk.value());
    assertEquals("MessageCount", 0, mc2879.value());
  }
}
