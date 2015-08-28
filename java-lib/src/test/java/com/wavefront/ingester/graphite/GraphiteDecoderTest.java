package com.wavefront.ingester.graphite;

import com.google.common.collect.Lists;
import org.junit.Ignore;
import org.junit.Test;
import sunnylabs.report.ReportPoint;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * TODO: Edit this
 * <p/>
 * User: sam
 * Date: 7/13/13
 * Time: 11:34 AM
 */
public class GraphiteDecoderTest {

  @Test
  public void testDoubleFormat() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder("localhost");
    List<Object> out = new ArrayList<Object>();
    decoder.decode(null, "tsdb.vehicle.charge.battery_level 93.123e3 host=vehicle_2554", out);
    ReportPoint point = (ReportPoint) out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93123.0, point.getValue());
    assertEquals("vehicle_2554", point.getHost());

    out = new ArrayList<Object>();
    decoder.decode(null, "tsdb.vehicle.charge.battery_level -93.123e3 host=vehicle_2554", out);
    point = (ReportPoint) out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(-93123.0, point.getValue());
    assertEquals("vehicle_2554", point.getHost());

    out = new ArrayList<Object>();
    decoder.decode(null, "tsdb.vehicle.charge.battery_level 93.123e-3 host=vehicle_2554", out);
    point = (ReportPoint) out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(0.093123, point.getValue());
    assertEquals("vehicle_2554", point.getHost());

    out = new ArrayList<Object>();
    decoder.decode(null, "tsdb.vehicle.charge.battery_level -93.123e-3 host=vehicle_2554", out);
    point = (ReportPoint) out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(-0.093123, point.getValue());
    assertEquals("vehicle_2554", point.getHost());

    List<ReportPoint> points = Lists.newArrayList();
    decoder.decodeReportPoints("test.devnag.10 100 host=ip1", points, "tsdb");
    point = points.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("test.devnag.10", point.getMetric());
    assertEquals(100.0, point.getValue());
    assertEquals("ip1", point.getHost());

    points.clear();
    decoder.decodeReportPoints("test.devnag.10 100 host=ip1 a=500", points, "tsdb");
    point = points.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("test.devnag.10", point.getMetric());
    assertEquals(100.0, point.getValue());
    assertEquals("ip1", point.getHost());
    assertEquals(1, point.getAnnotations().size());
    assertEquals("500", point.getAnnotations().get("a"));
    points.clear();
    decoder.decodeReportPoints("test.devnag.10 100 host=ip1 b=500", points, "tsdb");
    point = points.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("test.devnag.10", point.getMetric());
    assertEquals(100.0, point.getValue());
    assertEquals("ip1", point.getHost());
    assertEquals(1, point.getAnnotations().size());
    assertEquals("500", point.getAnnotations().get("b"));
    points.clear();
    decoder.decodeReportPoints("test.devnag.10 100 host=ip1 A=500", points, "tsdb");
    point = points.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("test.devnag.10", point.getMetric());
    assertEquals(100.0, point.getValue());
    assertEquals("ip1", point.getHost());
    assertEquals(1, point.getAnnotations().size());
    assertEquals("500", point.getAnnotations().get("A"));
  }

  @Test
  public void testFormat() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder("localhost");
    List<Object> out = new ArrayList<Object>();
    decoder.decode(null, "tsdb.vehicle.charge.battery_level 93 host=vehicle_2554", out);
    ReportPoint point = (ReportPoint) out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals("vehicle_2554", point.getHost());
  }

  @Test
  public void testIpV4Host() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder("localhost");
    List<Object> out = new ArrayList<Object>();
    decoder.decode(null, "tsdb.vehicle.charge.battery_level 93 host=10.0.0.1", out);
    ReportPoint point = (ReportPoint) out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals("10.0.0.1", point.getHost());
  }

  @Test
  public void testIpV6Host() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder("localhost");
    List<Object> out = new ArrayList<Object>();
    decoder.decode(null, "tsdb.vehicle.charge.battery_level 93 host=2001:db8:3333:4444:5555:6666:7777:8888", out);
    ReportPoint point = (ReportPoint) out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals("2001:db8:3333:4444:5555:6666:7777:8888", point.getHost());
  }

  @Test
  public void testFormatWithTimestamp() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder("localhost");
    List<Object> out = new ArrayList<Object>();
    decoder.decode(null, "tsdb.vehicle.charge.battery_level 93 1234567890.246 host=vehicle_2554", out);
    ReportPoint point = (ReportPoint) out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals(1234567890246L, point.getTimestamp().longValue());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals("vehicle_2554", point.getHost());
  }

  @Test
  public void testFormatWithTags() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder("localhost");
    List<Object> out = new ArrayList<Object>();
    decoder.decode(null, "tsdb.vehicle.charge.battery_level \"hello world\" host=machine tag1=value1 tag2=\"value2\" tag3=value3", out);
    ReportPoint point = (ReportPoint) out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals("hello world", point.getValue());
    assertEquals("machine", point.getHost());
    assertEquals("value1", point.getAnnotations().get("tag1"));
    assertEquals("value2", point.getAnnotations().get("tag2"));
    assertEquals("value3", point.getAnnotations().get("tag3"));
    assertEquals(3, point.getAnnotations().size());
  }

  @Test
  public void testFormatWithNoTags() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder("localhost");
    List<Object> out = new ArrayList<Object>();
    decoder.decode(null, "tsdb.vehicle.charge.battery_level 93", out);
    ReportPoint point = (ReportPoint) out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
  }

  @Test
  public void testFormatWithNoTagsAndTimestamp() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder("localhost");
    List<Object> out = new ArrayList<Object>();
    decoder.decode(null, "tsdb.vehicle.charge.battery_level 93 1234567890.246", out);
    ReportPoint point = (ReportPoint) out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals(1234567890246L, point.getTimestamp().longValue());
  }

  @Test
  public void testDecodeWithNoCustomer() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder();
    List<ReportPoint> out = Lists.newArrayList();
    Long rightNow = System.currentTimeMillis();
    decoder.decodeReportPoints("vehicle.charge.battery_level 93 host=vehicle_2554", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals("vehicle_2554", point.getHost());
  }

  @Test
  public void testDecodeWithNoCustomerWithTimestamp() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder();
    List<ReportPoint> out = Lists.newArrayList();
    Long rightNow = System.currentTimeMillis();
    decoder.decodeReportPoints("vehicle.charge.battery_level 93 1234567890.246 host=vehicle_2554", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals(1234567890246L, point.getTimestamp().longValue());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals("vehicle_2554", point.getHost());
  }

  @Test
  public void testDecodeWithNoCustomerWithNoTags() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder();
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("vehicle.charge.battery_level 93", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
  }

  @Test
  public void testDecodeWithNoCustomerWithNoTagsAndTimestamp() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder();
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("vehicle.charge.battery_level 93 1234567890.246", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals(1234567890246L, point.getTimestamp().longValue());
  }

  @Test
  public void testMetricWithNumberStarting() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder();
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("1vehicle.charge.battery_level 93 1234567890.246", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("1vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals(1234567890246L, point.getTimestamp().longValue());
  }

  @Test
  public void testMetricWithAnnotationQuoted() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder();
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("1vehicle.charge.battery_level 93 1234567890.246 host=12345 blah=\"test hello\" " +
        "\"hello world\"=test", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("1vehicle.charge.battery_level", point.getMetric());
    assertEquals("12345", point.getHost());
    assertEquals(93.0, point.getValue());
    assertEquals(1234567890246L, point.getTimestamp().longValue());
    assertEquals("test hello", point.getAnnotations().get("blah"));
    assertEquals("test", point.getAnnotations().get("hello world"));
  }

  @Test
  public void testQuotes() {
    GraphiteDecoder decoder = new GraphiteDecoder();
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("\"1vehicle.charge.'battery_level\" 93 1234567890.246 " +
        "host=12345 blah=\"test'\\\"hello\" \"hello world\"=test", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("1vehicle.charge.'battery_level", point.getMetric());
    assertEquals("12345", point.getHost());
    assertEquals(93.0, point.getValue());
    assertEquals(1234567890246L, point.getTimestamp().longValue());
    assertEquals("test'\"hello", point.getAnnotations().get("blah"));
    assertEquals("test", point.getAnnotations().get("hello world"));
  }

  @Test
  public void testSimple() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder();
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("test 1 host=test", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("test", point.getMetric());
    assertEquals("test", point.getHost());
    assertEquals(1.0, point.getValue());
  }

  @Test
  public void testSource() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder();
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("test 1 source=test", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("test", point.getMetric());
    assertEquals("test", point.getHost());
    assertEquals(1.0, point.getValue());
  }

  @Test
  public void testSourcePriority() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder();
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("test 1 source=test host=bar", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("test", point.getMetric());
    assertEquals("test", point.getHost());
    assertEquals("bar", point.getAnnotations().get("_host"));
    assertEquals(1.0, point.getValue());
  }

  @Test
  public void testTagRewrite() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder();
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("test 1 source=test tag=bar", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("test", point.getMetric());
    assertEquals("test", point.getHost());
    assertEquals("bar", point.getAnnotations().get("_tag"));
    assertEquals(1.0, point.getValue());
  }

  @Test
  public void testMONIT2576() {
    GraphiteDecoder decoder = new GraphiteDecoder();
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("vm.guest.virtualDisk.mediumSeeks.latest 4.00 1439250320 " +
        "host=iadprdhyp02.iad.xactlycorp.com guest=47173170-2069-4bcc-9bd4-041055b554ec " +
        "instance=ide0_0", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("vm.guest.virtualDisk.mediumSeeks.latest", point.getMetric());
    assertEquals("iadprdhyp02.iad.xactlycorp.com", point.getHost());
    assertEquals("47173170-2069-4bcc-9bd4-041055b554ec", point.getAnnotations().get("guest"));
    assertEquals("ide0_0", point.getAnnotations().get("instance"));
    assertEquals(4.0, point.getValue());

    out = new ArrayList<ReportPoint>();
    try {
      decoder.decodeReportPoints("test.metric 1 host=test test=\"", out, "customer");
      fail("should throw");
    } catch (Exception ignored) {
    }
  }

  @Ignore
  @Test
  public void testBenchmark() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder("localhost");
    int ITERATIONS = 1000000;
    for (int i = 0; i < ITERATIONS / 1000; i++) {
      List<Object> out = new ArrayList<Object>();
      decoder.decode(null, "tsdb.vehicle.charge.battery_level 93 123456 host=vehicle_2554", out);
    }
    long start = System.currentTimeMillis();
    for (int i = 0; i < ITERATIONS; i++) {
      List<Object> out = new ArrayList<Object>();
      decoder.decode(null, "tsdb.vehicle.charge.battery_level 93 123456 host=vehicle_2554", out);
    }
    double end = System.currentTimeMillis();
    System.out.println(ITERATIONS / ((end - start) / 1000) + " DPS");
  }
}
