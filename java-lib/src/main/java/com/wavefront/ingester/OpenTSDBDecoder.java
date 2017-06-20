package com.wavefront.ingester;

import com.google.common.base.Preconditions;

import java.util.List;

import wavefront.report.ReportPoint;

/**
 * OpenTSDB decoder that takes in a point of the type:
 *
 * PUT [metric] [timestamp] [value] [annotations]
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class OpenTSDBDecoder implements Decoder<String> {

  private final String hostName;
  private static final AbstractIngesterFormatter<ReportPoint> FORMAT =
      IngesterFormatter.newBuilder()
      .whiteSpace()
      .appendCaseInsensitiveLiteral("put").whiteSpace()
      .appendMetricName().whiteSpace()
      .appendTimestamp().whiteSpace()
      .appendValue().whiteSpace()
      .appendAnnotationsConsumer().whiteSpace().build();
  private List<String> customSourceTags;

  public OpenTSDBDecoder(List<String> customSourceTags) {
    this.hostName = "unknown";
    Preconditions.checkNotNull(customSourceTags);
    this.customSourceTags = customSourceTags;
  }

  public OpenTSDBDecoder(String hostName, List<String> customSourceTags) {
    Preconditions.checkNotNull(hostName);
    this.hostName = hostName;
    Preconditions.checkNotNull(customSourceTags);
    this.customSourceTags = customSourceTags;
  }

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out, String customerId) {
    ReportPoint point = FORMAT.drive(msg, hostName, customerId, customSourceTags);
    if (out != null) {
      out.add(point);
    }
  }

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out) {
    ReportPoint point = FORMAT.drive(msg, hostName, "dummy", customSourceTags);
    if (out != null) {
      out.add(point);
    }
  }

  public String getDefaultHostName() {
    return this.hostName;
  }
}
