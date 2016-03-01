package com.wavefront.ingester;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import sunnylabs.report.ReportPoint;

/**
 * OpenTSDB decoder that takes in a point of the type:
 *
 * PUT [metric] [timestamp] [value] [annotations]
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class OpenTSDBDecoder implements Decoder {
  protected static final Logger logger = Logger.getLogger("OpenTSDBDecoder");

  private final String hostName;
  private static final IngesterFormatter FORMAT = IngesterFormatter.newBuilder().whiteSpace()
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
    logger.fine("Decoding OpenTSDB point " + msg);
    ReportPoint point = FORMAT.drive(msg, hostName, customerId, customSourceTags);
    if (out != null) {
      out.add(point);
    }
  }

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out) {
    logger.fine("Decoding OpenTSDB point " + msg);
    ReportPoint point = FORMAT.drive(msg, hostName, "dummy", customSourceTags);
    if (out != null) {
      out.add(point);
    }
  }

  @Override
  public String toString() {
    return "Open TSDB Decoder";
  }
}
