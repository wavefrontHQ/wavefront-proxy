package com.wavefront.ingester;

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static junit.framework.Assert.assertEquals;

/**
 * Tests for GraphiteHostAnnotator.
 *
 * @author Conor Beverland (conor@wavefront.com).
 */
public class GraphiteHostAnnotatorTest {

  @Test
  public void testHostMatches() throws Exception {
    GraphiteHostAnnotator handler = new GraphiteHostAnnotator("test.host.com");
    List<Object> out = new LinkedList<Object>();
    String msg = "test.metric 1 host=foo";
    handler.decode(null, msg, out);
    assertEquals(msg, out.get(0));
  }

  @Test
  public void testSourceMatches() throws Exception {
    GraphiteHostAnnotator handler = new GraphiteHostAnnotator("test.host.com");
    List<Object> out = new LinkedList<Object>();
    String msg = "test.metric 1 source=foo";
    handler.decode(null, msg, out);
    assertEquals(msg, out.get(0));
  }

  @Test
  public void testSourceAdded() throws Exception {
    GraphiteHostAnnotator handler = new GraphiteHostAnnotator("test.host.com");
    List<Object> out = new LinkedList<Object>();
    String msg = "test.metric 1";
    handler.decode(null, msg, out);
    assertEquals("test.metric 1 source=test.host.com", out.get(0));
  }
}
