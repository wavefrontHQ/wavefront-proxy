package com.wavefront.ingester;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.concurrent.TimeUnit;

import sunnylabs.report.ReportPoint;

/**
 * OpenTSDB decoder that takes in a point of the type:
 *
 * PUT [metric] [timestamp] [value] [annotations]
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class OpenTSDBDecoder implements Decoder {

  private final String hostName;
  private static final IngesterFormatter FORMAT = IngesterFormatter.newBuilder()
      .appendCaseInsensitiveLiteral("put").whiteSpace()
      .appendMetricName().whiteSpace()
      .appendTimestamp(TimeUnit.SECONDS).whiteSpace()
      .appendValue().whiteSpace()
      .appendAnnotationsConsumer().build();

  public OpenTSDBDecoder() {
    this.hostName = "unknown";
  }

  public OpenTSDBDecoder(String hostName) {
    Preconditions.checkNotNull(hostName);
    this.hostName = hostName;
  }

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out, String customerId) {
    ReportPoint point = FORMAT.drive(msg, hostName, customerId);
    if (out != null) {
      out.add(point);
    }
  }

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out) {
    ReportPoint point = FORMAT.drive(msg, hostName, "dummy");
    if (out != null) {
      out.add(point);
    }
  }
}
