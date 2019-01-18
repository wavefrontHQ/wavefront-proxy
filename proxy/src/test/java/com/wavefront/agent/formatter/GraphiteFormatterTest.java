package com.wavefront.agent.formatter;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Andrew Kao (andrew@wavefront.com)
 */
public class GraphiteFormatterTest {

  private static final Logger logger = LoggerFactory.getLogger(GraphiteFormatterTest.class);

  @Test
  public void testCollectdGraphiteParsing() {
    String format = "4,3,2"; // Extract the 4th, 3rd, and 2nd segments of the metric as the hostname, in that order
    String format2 = "2";
    String delimiter = "_";

    // Test input
    String testString1 = "collectd.com.bigcorp.www02_web.cpu.loadavg.1m 40";
    String testString2 = "collectd.com.bigcorp.www02_web.cpu.loadavg.1m 40 1415233342";
    String testString3 = "collectd.almost.too.short 40 1415233342";
    String testString4 = "collectd.too.short 40 1415233342";
    String testString5 = "collectd.www02_web_bigcorp_com.cpu.loadavg.1m;context=abc;hostname=www02.web.bigcorp.com 40 1415233342";

    // Test output
    String expected1 = "collectd.cpu.loadavg.1m 40 source=www02.web.bigcorp.com";
    String expected2 = "collectd.cpu.loadavg.1m 40 1415233342 source=www02.web.bigcorp.com";
    String expected5 = "collectd.cpu.loadavg.1m 40 1415233342 source=www02.web.bigcorp.com context=abc hostname=www02.web.bigcorp.com";

    // Test basic functionality with correct input
    GraphiteFormatter formatter = new GraphiteFormatter(format, delimiter, "");
    String output1 = formatter.apply(testString1);
    assertEquals(expected1, output1);
    String output2 = formatter.apply(testString2);
    assertEquals(expected2, output2);

    // Test format length limits
    formatter.apply(testString3); // should not throw exception

    // Do we properly reject metrics that don't work with the given format?
    boolean threwException = false;
    try {
      formatter.apply(testString4); // should be too short for given format
    } catch (IllegalArgumentException e) {
      threwException = true;
    }
    assertTrue(threwException);

    // Do we properly reject invalid formats?
    String badFormat = "4,2,0"; // nuh-uh; we're doing 1-based indexing
    threwException = false;
    try {
      new GraphiteFormatter(badFormat, delimiter, "");
    } catch (IllegalArgumentException e) {
      threwException = true;
    }
    assertTrue(threwException);

    // Benchmark
    long start = System.nanoTime();
    for (int index = 0; index < 1000 * 1000; index++) {
      formatter.apply(testString2);
    }
    long end = System.nanoTime();

    // Report/validate performance
    logger.error(" Time to parse 1M strings: " + (end - start) + " ns for " + formatter.getOps() + " runs");
    long nsPerOps = (end - start) / formatter.getOps();
    logger.error(" ns per op: " + nsPerOps + " and ops/sec " + (1000 * 1000 * 1000 / nsPerOps));
    assertTrue(formatter.getOps() >= 1000 * 1000);  // make sure we actually ran it 1M times
    assertTrue(nsPerOps < 10 * 1000); // make sure it was less than 10 μs per run; it's around 1 μs on my machine

    // new addition to test the point tags inside the metric names
    formatter = new GraphiteFormatter(format2, delimiter, "");
    String output5 = formatter.apply(testString5);
    assertEquals(expected5, output5);
  }

  @Test
  public void testFieldsToRemove() {
    String format = "2"; // Extract the 2nd field for host name
    String delimiter = "_";
    String remove = "1,3";  // remove the 1st and 3rd fields from metric name

    // Test input
    String testString1 = "hosts.host1.collectd.cpu.loadavg.1m 40";
    String testString2 = "hosts.host1.collectd.cpu.loadavg.1m 7 1459527231";

    // Test output
    String expected1 = "cpu.loadavg.1m 40 source=host1";
    String expected2 = "cpu.loadavg.1m 7 1459527231 source=host1";

    GraphiteFormatter formatter = new GraphiteFormatter(format, delimiter, remove);
    assertEquals(expected1, formatter.apply(testString1));
    assertEquals(expected2, formatter.apply(testString2));
  }

  @Test
  public void testFieldsToRemoveInvalidFormats() {
    String format = "2"; // Extract the 2nd field for host name
    String delimiter = "_";

    // Test input
    String testString1 = "hosts.host1.collectd.cpu.loadavg.1m 40";
    String testString2 = "hosts.host1.collectd.cpu.loadavg.1m 7 1459527231";

    // empty string
    GraphiteFormatter formatter = new GraphiteFormatter(format, delimiter, ",");

    // Test output
    String expected1 = "hosts.collectd.cpu.loadavg.1m 40 source=host1";
    String expected2 = "hosts.collectd.cpu.loadavg.1m 7 1459527231 source=host1";

    assertEquals(expected1, formatter.apply(testString1));
    assertEquals(expected2, formatter.apply(testString2));

    // <= 0 number
    try {
      new GraphiteFormatter(format, delimiter, "0");
      fail("Expected fields to remove value of '0' to fail");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}