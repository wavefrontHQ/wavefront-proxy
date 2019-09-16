package com.wavefront.agent.common;

import com.wavefront.common.HostMetricTagsPair;

import org.junit.Assert;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Jia Deng (djia@vmware.com)
 */
public class HostMetricTagsPairTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testEmptyHost() {
    thrown.expect(NullPointerException.class);
    Map<String, String> tags = new HashMap<>();
    tags.put("key", "value");
    new HostMetricTagsPair(null, "metric", tags);
  }

  @Test
  public void testEmptyMetric() {
    Map<String, String> tags = new HashMap<>();
    tags.put("key", "value");
    thrown.expect(NullPointerException.class);
    new HostMetricTagsPair("host", null, tags);
  }

  @Test
  public void testGetMetric() {
    HostMetricTagsPair hostMetricTagsPair = new HostMetricTagsPair("host", "metric",
        null);
    Assert.assertEquals(hostMetricTagsPair.getMetric(), "metric");
  }

  @Test
  public void testGetHost() {
    HostMetricTagsPair hostMetricTagsPair = new HostMetricTagsPair("host", "metric",
        null);
    Assert.assertEquals(hostMetricTagsPair.getHost(), "host");
  }

  @Test
  public void testGetTags() {
    Map<String, String> tags = new HashMap<>();
    tags.put("key", "value");
    HostMetricTagsPair hostMetricTagsPair = new HostMetricTagsPair("host", "metric", tags);
    Assert.assertEquals(hostMetricTagsPair.getTags(), tags);
  }

  @Test
  public void testEquals() throws Exception {
    Map<String, String> tags1 = new HashMap<>();
    tags1.put("key1", "value1");
    HostMetricTagsPair hostMetricTagsPair1 = new HostMetricTagsPair("host1", "metric1",
        tags1);

    //equals itself
    Assert.assertTrue(hostMetricTagsPair1.equals(hostMetricTagsPair1));

    //same hostMetricTagsPair
    HostMetricTagsPair hostMetricTagsPair2 = new HostMetricTagsPair("host1", "metric1",
        tags1);
    Assert.assertTrue(hostMetricTagsPair1.equals(hostMetricTagsPair2));

    //compare different host with hostMetricTagsPair1
    HostMetricTagsPair hostMetricTagsPair3 = new HostMetricTagsPair("host2", "metric1",
        tags1);
    Assert.assertFalse(hostMetricTagsPair1.equals(hostMetricTagsPair3));

    //compare different metric with hostMetricTagsPair1
    HostMetricTagsPair hostMetricTagsPair4 = new HostMetricTagsPair("host1", "metric2",
        tags1);
    Assert.assertFalse(hostMetricTagsPair1.equals(hostMetricTagsPair4));

    //compare different tags with hostMetricTagsPair1
    Map<String, String> tags2 = new HashMap<>();
    tags2.put("key2", "value2");
    HostMetricTagsPair hostMetricTagsPair5 = new HostMetricTagsPair("host1", "metric1",
        tags2);
    Assert.assertFalse(hostMetricTagsPair1.equals(hostMetricTagsPair5));

    //compare empty tags with hostMetricTagsPair1
    HostMetricTagsPair hostMetricTagsPair6 = new HostMetricTagsPair("host1", "metric1",
        null);
    Assert.assertFalse(hostMetricTagsPair1.equals(hostMetricTagsPair6));
  }

  @Test
  public void testToString() throws Exception {
    Map<String, String> tags = new HashMap<>();
    tags.put("key", "value");
    HostMetricTagsPair hostMetricTagsPair = new HostMetricTagsPair("host", "metric", tags);
    Assert.assertEquals(hostMetricTagsPair.toString(), "[host: host, metric: metric, tags: " +
        "{key=value}]");
  }
}
