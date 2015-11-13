package com.wavefront.ingester;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import sunnylabs.report.ReportPoint;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for OpenTSDBDecoder.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class OpenTSDBDecoderTest {

  @Test
  public void testDoubleFormat() throws Exception {
    OpenTSDBDecoder decoder = new OpenTSDBDecoder("localhost");
    List<ReportPoint> out = new ArrayList<>();
    decoder.decodeReportPoints("put tsdb.vehicle.charge.battery_level 12345.678 93.123e3 host=vehicle_2554", out);
    ReportPoint point = out.get(0);
    assertEquals("dummy", point.getTable());
    assertEquals("tsdb.vehicle.charge.battery_level", point.getMetric());
    assertEquals(93123.0, point.getValue());
    assertEquals(12345678L, point.getTimestamp().longValue());
    assertEquals("vehicle_2554", point.getHost());

    try {
      // need "PUT"
      decoder.decodeReportPoints("tsdb.vehicle.charge.battery_level 12345.678 93.123e3 host=vehicle_2554", out);
      fail();
    } catch (Exception ex) {
    }

    try {
      // need "timestamp"
      decoder.decodeReportPoints("put tsdb.vehicle.charge.battery_level 93.123e3 host=vehicle_2554", out);
      fail();
    } catch (Exception ex) {
    }

    try {
      // need "value"
      decoder.decodeReportPoints("put tsdb.vehicle.charge.battery_level 12345.678 host=vehicle_2554", out);
      fail();
    } catch (Exception ex) {
    }

    out = new ArrayList<>();
    decoder.decodeReportPoints("put tsdb.vehicle.charge.battery_level 12345.678 93.123e3", out);
    point = out.get(0);
    assertEquals("dummy", point.getTable());
    assertEquals("tsdb.vehicle.charge.battery_level", point.getMetric());
    assertEquals(93123.0, point.getValue());
    assertEquals(12345678L, point.getTimestamp().longValue());
    assertEquals("localhost", point.getHost());
  }
}
