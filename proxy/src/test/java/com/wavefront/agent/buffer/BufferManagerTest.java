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
  public void initTest() throws InterruptedException, IOException {
    Path buffer = Files.createTempDirectory("wfproxy");
    System.out.println("buffer: " + buffer);

    BuffersManager.init(buffer.toFile().getAbsolutePath());
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
  }
}
