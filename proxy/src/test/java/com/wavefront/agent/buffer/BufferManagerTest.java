package com.wavefront.agent.buffer;

import static org.junit.Assert.assertEquals;

import com.yammer.metrics.core.Gauge;
import java.util.Collections;
import org.junit.Test;

public class BufferManagerTest {
  @Test
  public void initTest() {
    BufferManager.init();
    BufferManager.registerNewPort("2878");
    BufferManager.registerNewPort("2879");

    Gauge mc2878 = BufferManager.getMcGauge("2878");
    Gauge mc2879 = BufferManager.getMcGauge("2879");

    assertEquals("MessageCount", 0, mc2878.value());
    assertEquals("MessageCount", 0, mc2879.value());

    BufferManager.sendMsg("2878", Collections.singletonList("tururu"));
    BufferManager.sendMsg("2879", Collections.singletonList("tururu2"));

    assertEquals("MessageCount", 1, mc2878.value());
    assertEquals("MessageCount", 1, mc2879.value());

    // force MSG to DL
    for (int i = 0; i < 3; i++) {
      assertEquals("MessageCount", 1, mc2879.value());
      BufferManager.onMsg(
          "2879",
          msg -> {
            assertEquals("MessageCount", "tururu2", msg);
            throw new Exception("error");
          });
    }

    assertEquals("MessageCount", 1, mc2878.value());
    assertEquals("MessageCount", 0, mc2879.value());

    BufferManager.onMsg("2878", msg -> assertEquals("MessageCount", "tururu", msg));

    assertEquals("MessageCount", 0, mc2878.value());
    assertEquals("MessageCount", 0, mc2879.value());
  }
}
