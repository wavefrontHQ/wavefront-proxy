package com.wavefront.ingester;

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static junit.framework.Assert.assertEquals;

/**
 * Tests for GraphiteHostAnnotator.
 *
 * @author Conor Beverland (conor@wavefront.com).
 */
public class GraphiteHostAnnotatorTest {

  private final static List<String> emptyCustomSourceTags = Collections.emptyList();

  @Test
  public void testHostMatches() throws Exception {
    GraphiteHostAnnotator handler = new GraphiteHostAnnotator("test.host.com", emptyCustomSourceTags);
    List<Object> out = new LinkedList<Object>();
    String msg = "test.metric 1 host=foo";
    handler.decode(null, msg, out);
    assertEquals(msg, out.get(0));
  }

  @Test
  public void testSourceMatches() throws Exception {
    GraphiteHostAnnotator handler = new GraphiteHostAnnotator("test.host.com", emptyCustomSourceTags);
    List<Object> out = new LinkedList<Object>();
    String msg = "test.metric 1 source=foo";
    handler.decode(null, msg, out);
    assertEquals(msg, out.get(0));
  }

  @Test
  public void testSourceAdded() throws Exception {
    GraphiteHostAnnotator handler = new GraphiteHostAnnotator("test.host.com", emptyCustomSourceTags);
    List<Object> out = new LinkedList<Object>();
    String msg = "test.metric 1";
    handler.decode(null, msg, out);
    assertEquals("test.metric 1 source=test.host.com", out.get(0));
  }

  @Test
  public void testCustomTagMatches() throws Exception {
    List<String> customSourceTags = new ArrayList<String>();
    customSourceTags.add("fqdn");
    customSourceTags.add("hostname");
    GraphiteHostAnnotator handler = new GraphiteHostAnnotator("test.host.com", customSourceTags);
    List<Object> out = new LinkedList<Object>();
    String msg = "test.metric 1 fqdn=test";
    handler.decode(null, msg, out);
    assertEquals(msg, out.get(0));
  }

  @Test
  public void testSourceAddedWithCustomTags() throws Exception {
    List<String> customSourceTags = new ArrayList<String>();
    customSourceTags.add("fqdn");
    customSourceTags.add("hostname");
    GraphiteHostAnnotator handler = new GraphiteHostAnnotator("test.host.com", customSourceTags);
    List<Object> out = new LinkedList<Object>();
    String msg = "test.metric 1 foo=bar";
    handler.decode(null, msg, out);
    assertEquals("test.metric 1 foo=bar source=\"test.host.com\"", out.get(0));
  }

  @Ignore
  @Test
  public void testBenchmark() throws Exception {
    List<String> customSourceTags = new ArrayList<String>();
    customSourceTags.add("fqdn");
    customSourceTags.add("hostname");
    customSourceTags.add("highcardinalitytag1");
    customSourceTags.add("highcardinalitytag2");
    customSourceTags.add("highcardinalitytag3");
    customSourceTags.add("highcardinalitytag4");
    customSourceTags.add("highcardinalitytag5");

    GraphiteHostAnnotator handler = new GraphiteHostAnnotator("test.host.com", customSourceTags);
    int ITERATIONS = 1000000;

    for (int i = 0; i < ITERATIONS / 1000; i++) {
      List<Object> out = new ArrayList<>();
      handler.decode(null, "tsdb.vehicle.charge.battery_level 93 123456 host=vehicle_2554", out);
    }
    long start = System.currentTimeMillis();
    for (int i = 0; i < ITERATIONS; i++) {
      List<Object> out = new ArrayList<>();
      handler.decode(null, "tsdb.vehicle.charge.battery_level 93 123456 host=vehicle_2554", out);
    }
    double end = System.currentTimeMillis();
    System.out.println(ITERATIONS / ((end - start) / 1000) + " DPS");
  }
}
