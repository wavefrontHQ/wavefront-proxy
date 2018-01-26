package com.wavefront.ingester;

import com.google.common.collect.Lists;

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import wavefront.report.ReportPoint;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * TODO: Edit this <p/> User: sam Date: 7/13/13 Time: 11:34 AM
 */
public class GraphiteDecoderTest {

  private final static List<String> emptyCustomSourceTags = Collections.emptyList();

  @Test
  public void testDoubleFormat() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder("localhost", emptyCustomSourceTags);
    List<ReportPoint> out = new ArrayList<>();
    decoder.decodeReportPoints("tsdb.vehicle.charge.battery_level 93.123e3 host=vehicle_2554", out);
    ReportPoint point = out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93123.0, point.getValue());
    assertEquals("vehicle_2554", point.getHost());

    out = new ArrayList<>();
    decoder.decodeReportPoints("tsdb.vehicle.charge.battery_level -93.123e3 host=vehicle_2554", out);
    point = out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(-93123.0, point.getValue());
    assertEquals("vehicle_2554", point.getHost());

    out = new ArrayList<>();
    decoder.decodeReportPoints("tsdb.vehicle.charge.battery_level -93.123e3", out);
    point = out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(-93123.0, point.getValue());
    assertEquals("localhost", point.getHost());
    assertNotNull(point.getAnnotations());
    assertTrue(point.getAnnotations().isEmpty());

    out = new ArrayList<>();
    decoder.decodeReportPoints("tsdb.vehicle.charge.battery_level 93.123e-3 host=vehicle_2554", out);
    point = out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(0.093123, point.getValue());
    assertEquals("vehicle_2554", point.getHost());

    out = new ArrayList<>();
    decoder.decodeReportPoints("tsdb.vehicle.charge.battery_level -93.123e-3 host=vehicle_2554", out);
    point = out.get(0);
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
  public void testTagVWithDigitAtBeginning() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder("localhost", emptyCustomSourceTags);
    List<ReportPoint> out = new ArrayList<>();
    decoder.decodeReportPoints("tsdb.vehicle.charge.battery_level 93 host=vehicle_2554 version=1_0", out);
    ReportPoint point = out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals("vehicle_2554", point.getHost());
    assertEquals("1_0", point.getAnnotations().get("version"));
  }

  @Test
  public void testFormat() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder("localhost", emptyCustomSourceTags);
    List<ReportPoint> out = new ArrayList<>();
    decoder.decodeReportPoints("tsdb.vehicle.charge.battery_level 93 host=vehicle_2554", out);
    ReportPoint point = out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals("vehicle_2554", point.getHost());
  }

  @Test
  public void testIpV4Host() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder("localhost", emptyCustomSourceTags);
    List<ReportPoint> out = new ArrayList<>();
    decoder.decodeReportPoints("tsdb.vehicle.charge.battery_level 93 host=10.0.0.1", out);
    ReportPoint point = out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals("10.0.0.1", point.getHost());
  }

  @Test
  public void testIpV6Host() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder("localhost", emptyCustomSourceTags);
    List<ReportPoint> out = new ArrayList<>();
    decoder.decodeReportPoints("tsdb.vehicle.charge.battery_level 93 host=2001:db8:3333:4444:5555:6666:7777:8888", out);
    ReportPoint point = out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals("2001:db8:3333:4444:5555:6666:7777:8888", point.getHost());
  }

  @Test
  public void testFormatWithTimestamp() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder("localhost", emptyCustomSourceTags);
    List<ReportPoint> out = new ArrayList<>();
    decoder.decodeReportPoints("tsdb.vehicle.charge.battery_level 93 1234567890.246 host=vehicle_2554", out);
    ReportPoint point = out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals(1234567890246L, point.getTimestamp().longValue());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals("vehicle_2554", point.getHost());
  }

  @Test
  public void testFormatWithNoTags() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder("localhost", emptyCustomSourceTags);
    List<ReportPoint> out = new ArrayList<>();
    decoder.decodeReportPoints("tsdb.vehicle.charge.battery_level 93", out);
    ReportPoint point = out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
  }

  @Test
  public void testFormatWithNoTagsAndTimestamp() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder("localhost", emptyCustomSourceTags);
    List<ReportPoint> out = new ArrayList<>();
    decoder.decodeReportPoints("tsdb.vehicle.charge.battery_level 93 1234567890.246", out);
    ReportPoint point = out.get(0);
    assertEquals("tsdb", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals(1234567890246L, point.getTimestamp().longValue());
  }

  @Test
  public void testDecodeWithNoCustomer() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder(emptyCustomSourceTags);
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("vehicle.charge.battery_level 93 host=vehicle_2554", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals("vehicle_2554", point.getHost());
  }

  @Test
  public void testDecodeWithNoCustomerWithTimestamp() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder(emptyCustomSourceTags);
    List<ReportPoint> out = Lists.newArrayList();
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
    GraphiteDecoder decoder = new GraphiteDecoder(emptyCustomSourceTags);
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("vehicle.charge.battery_level 93", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
  }

  @Test
  public void testDecodeWithNoCustomerWithNoTagsAndTimestamp() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder(emptyCustomSourceTags);
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("vehicle.charge.battery_level 93 1234567890.246", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals(1234567890246L, point.getTimestamp().longValue());
  }

  @Test
  public void testDecodeWithMillisTimestamp() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder(emptyCustomSourceTags);
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("vehicle.charge.battery_level 93 1234567892468", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals(1234567892468L, point.getTimestamp().longValue());
  }

  @Test
  public void testMetricWithNumberStarting() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder(emptyCustomSourceTags);
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("1vehicle.charge.battery_level 93 1234567890.246", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("1vehicle.charge.battery_level", point.getMetric());
    assertEquals(93.0, point.getValue());
    assertEquals(1234567890246L, point.getTimestamp().longValue());
  }

  @Test
  public void testQuotedMetric() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder(emptyCustomSourceTags);
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("\"1vehicle.charge.$()+battery_level\" 93 1234567890.246 host=12345 " +
        "blah=\"test hello\" \"hello world\"=test", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("1vehicle.charge.$()+battery_level", point.getMetric());
    assertEquals("12345", point.getHost());
    assertEquals(93.0, point.getValue());
    assertEquals(1234567890246L, point.getTimestamp().longValue());
    assertEquals("test hello", point.getAnnotations().get("blah"));
    assertEquals("test", point.getAnnotations().get("hello world"));
  }

  @Test
  public void testMetricWithAnnotationQuoted() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder(emptyCustomSourceTags);
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
    GraphiteDecoder decoder = new GraphiteDecoder(emptyCustomSourceTags);
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
    GraphiteDecoder decoder = new GraphiteDecoder(emptyCustomSourceTags);
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
    GraphiteDecoder decoder = new GraphiteDecoder(emptyCustomSourceTags);
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
    List<String> customSourceTags = new ArrayList<String>();
    customSourceTags.add("fqdn");
    GraphiteDecoder decoder = new GraphiteDecoder(customSourceTags);
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("test 1 source=test host=bar fqdn=foo", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("test", point.getMetric());
    assertEquals("test", point.getHost());
    assertEquals("bar", point.getAnnotations().get("_host"));
    assertEquals("foo", point.getAnnotations().get("fqdn"));
    assertEquals(1.0, point.getValue());
  }

  @Test
  public void testFQDNasSource() throws Exception {
    List<String> customSourceTags = new ArrayList<String>();
    customSourceTags.add("fqdn");
    customSourceTags.add("hostname");
    GraphiteDecoder decoder = new GraphiteDecoder(customSourceTags);
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("test 1 hostname=machine fqdn=machine.company.com", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("test", point.getMetric());
    assertEquals("machine.company.com", point.getHost());
    assertEquals("machine.company.com", point.getAnnotations().get("fqdn"));
    assertEquals("machine", point.getAnnotations().get("hostname"));
    assertEquals(1.0, point.getValue());
  }


  @Test
  public void testUserPrefOverridesDefault() throws Exception {
    List<String> customSourceTags = new ArrayList<String>();
    customSourceTags.add("fqdn");
    GraphiteDecoder decoder = new GraphiteDecoder("localhost", customSourceTags);
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("test 1 fqdn=machine.company.com", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("test", point.getMetric());
    assertEquals("machine.company.com", point.getHost());
    assertEquals("machine.company.com", point.getAnnotations().get("fqdn"));
    assertEquals(1.0, point.getValue());
  }

  @Test
  public void testTagRewrite() throws Exception {
    GraphiteDecoder decoder = new GraphiteDecoder(emptyCustomSourceTags);
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
    GraphiteDecoder decoder = new GraphiteDecoder(emptyCustomSourceTags);
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("vm.guest.virtualDisk.mediumSeeks.latest 4.00 1439250320 " +
        "host=iadprdhyp02.iad.corp.com guest=47173170-2069-4bcc-9bd4-041055b554ec " +
        "instance=ide0_0", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("vm.guest.virtualDisk.mediumSeeks.latest", point.getMetric());
    assertEquals("iadprdhyp02.iad.corp.com", point.getHost());
    assertEquals("47173170-2069-4bcc-9bd4-041055b554ec", point.getAnnotations().get("guest"));
    assertEquals("ide0_0", point.getAnnotations().get("instance"));
    assertEquals(4.0, point.getValue());

    out = new ArrayList<>();
    try {
      decoder.decodeReportPoints("test.metric 1 host=test test=\"", out, "customer");
      fail("should throw");
    } catch (Exception ignored) {
    }
  }

  @Test
  public void testNumberLookingTagValue() {
    GraphiteDecoder decoder = new GraphiteDecoder(emptyCustomSourceTags);
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("vm.guest.virtualDisk.mediumSeeks.latest 4.00 1439250320 " +
        "host=iadprdhyp02.iad.corp.com version=\"1.0.0-030051.d0e485f\"", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("vm.guest.virtualDisk.mediumSeeks.latest", point.getMetric());
    assertEquals("iadprdhyp02.iad.corp.com", point.getHost());
    assertEquals("1.0.0-030051.d0e485f", point.getAnnotations().get("version"));
    assertEquals(4.0, point.getValue());
  }

  @Test
  public void testNumberLookingTagValue2() {
    GraphiteDecoder decoder = new GraphiteDecoder(emptyCustomSourceTags);
    List<ReportPoint> out = Lists.newArrayList();
    decoder.decodeReportPoints("vm.guest.virtualDisk.mediumSeeks.latest 4.00 1439250320 " +
        "host=iadprdhyp02.iad.corp.com version=\"1.0.0\\\"-030051.d0e485f\"", out, "customer");
    ReportPoint point = out.get(0);
    assertEquals("customer", point.getTable());
    assertEquals("vm.guest.virtualDisk.mediumSeeks.latest", point.getMetric());
    assertEquals("iadprdhyp02.iad.corp.com", point.getHost());
    assertEquals("1.0.0\"-030051.d0e485f", point.getAnnotations().get("version"));
    assertEquals(4.0, point.getValue());
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
    GraphiteDecoder decoder = new GraphiteDecoder("localhost", emptyCustomSourceTags);
    int ITERATIONS = 1000000;
    for (int i = 0; i < ITERATIONS / 1000; i++) {
      List<ReportPoint> out = new ArrayList<>();
      decoder.decodeReportPoints("tsdb.vehicle.charge.battery_level 93 123456 highcardinalitytag5=vehicle_2554", out);
    }
    long start = System.currentTimeMillis();
    for (int i = 0; i < ITERATIONS; i++) {
      List<ReportPoint> out = new ArrayList<>();
      decoder.decodeReportPoints("tsdb.vehicle.charge.battery_level 93 123456 highcardinalitytag5=vehicle_2554", out);
    }
    double end = System.currentTimeMillis();
    System.out.println(ITERATIONS / ((end - start) / 1000) + " DPS");
  }
}
