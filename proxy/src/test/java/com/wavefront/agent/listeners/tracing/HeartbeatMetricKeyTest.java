package com.wavefront.agent.listeners.tracing;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Unit tests for HeartbeatMetricKey
 *
 * @author Sushant Dewan (sushant@wavefront.com).
 */
public class HeartbeatMetricKeyTest {

  @Test
  public void testEqual() {
    HeartbeatMetricKey key1 = new HeartbeatMetricKey("app", "service", "cluster", "shard",
        "source");
    HeartbeatMetricKey key2 = new HeartbeatMetricKey("app", "service", "cluster", "shard",
        "source");
    assertEquals(key1, key2);

    assertEquals(key1.hashCode(), key2.hashCode());
  }

  @Test
  public void testNotEqual() {
    HeartbeatMetricKey key1 = new HeartbeatMetricKey("app1", "service", "cluster", "shard",
        "source");
    HeartbeatMetricKey key2 = new HeartbeatMetricKey("app2", "service", "none", "shard",
        "source");
    assertNotEquals(key1.hashCode(), key2.hashCode());
    assertNotEquals(key1, key2);
  }
}
