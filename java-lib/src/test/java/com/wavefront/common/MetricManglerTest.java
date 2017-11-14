package com.wavefront.common;

import static org.junit.Assert.*;
import org.junit.Test;

public class MetricManglerTest {
  @Test
  public void testSourceMetricParsing() {
    String testInput = "hosts.sjc123.cpu.loadavg.1m";
    String expectedOutput = "cpu.loadavg.1m";

    {
      MetricMangler mangler = new MetricMangler("2", "", "1");
      MetricMangler.MetricComponents c = mangler.extractComponents(testInput);
      assertEquals(expectedOutput, c.metric);
      assertEquals("sjc123", c.source);
    }
    {
      MetricMangler mangler = new MetricMangler("2", null, "1");
      MetricMangler.MetricComponents c = mangler.extractComponents(testInput);
      assertEquals(expectedOutput, c.metric);
      assertEquals("sjc123", c.source);
    }
  }

  @Test
  public void testSourceMetricParsingNoRemove() {
    String testInput = "hosts.sjc123.cpu.loadavg.1m";
    String expectedOutput = "hosts.cpu.loadavg.1m";

    {
      MetricMangler mangler = new MetricMangler("2", "", "");
      MetricMangler.MetricComponents c = mangler.extractComponents(testInput);
      assertEquals(expectedOutput, c.metric);
      assertEquals("sjc123", c.source);
    }
    {
      MetricMangler mangler = new MetricMangler("2", null, null);
      MetricMangler.MetricComponents c = mangler.extractComponents(testInput);
      assertEquals(expectedOutput, c.metric);
      assertEquals("sjc123", c.source);
    }
  }

  @Test
  public void testSourceMetricParsingNoSourceAndNoRemove() {
    String testInput = "hosts.sjc123.cpu.loadavg.1m";
    String expectedOutput = "hosts.sjc123.cpu.loadavg.1m";

    {
      MetricMangler mangler = new MetricMangler("", "", "");
      MetricMangler.MetricComponents c = mangler.extractComponents(testInput);
      assertEquals(expectedOutput, c.metric);
      assertEquals(null, c.source);
    }
    {
      MetricMangler mangler = new MetricMangler(null, null, null);
      MetricMangler.MetricComponents c = mangler.extractComponents(testInput);
      assertEquals(expectedOutput, c.metric);
      assertEquals(null, c.source);
    }
  }

  @Test
  public void testSourceMetricParsingWithTags() {
    String testInput = "hosts.sjc123.cpu.loadavg.1m;foo=bar;boo=baz";
    String expectedOutput = "cpu.loadavg.1m";

    {
      MetricMangler mangler = new MetricMangler("2", "", "1");
      MetricMangler.MetricComponents c = mangler.extractComponents(testInput);
      assertEquals(expectedOutput, c.metric);
      assertEquals("sjc123", c.source);
      assertNotEquals(null, c.annotations);
      assertEquals(2, c.annotations.length);
      assertEquals("foo=bar", c.annotations[0]);
      assertEquals("boo=baz", c.annotations[1]);
    }
  }


}
