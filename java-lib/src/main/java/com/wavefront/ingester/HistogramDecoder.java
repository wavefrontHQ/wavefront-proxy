package com.wavefront.ingester;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import sunnylabs.report.ReportPoint;

/**
 * Decoder that takes in histograms of the type:
 *
 * [BinType] [Timestamp] [Centroids] [Metric] [Annotations]
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class HistogramDecoder implements Decoder<String> {
  private static final Logger logger = Logger.getLogger(HistogramDecoder.class.getCanonicalName());
  private static final AbstractIngesterFormatter<ReportPoint> FORMAT =
      IngesterFormatter.newBuilder()
      .whiteSpace()
      .binType()
      .whiteSpace()
      .appendOptionalTimestamp()
      .adjustTimestamp()
      .whiteSpace()
      .centroids()
      .whiteSpace()
      .appendMetricName()
      .whiteSpace()
      .appendAnnotationsConsumer()
      .build();

  private final String defaultHostName;

  public HistogramDecoder() {
    this("unknown");
  }

  public HistogramDecoder(String defaultHostName) {
    this.defaultHostName = defaultHostName;
  }


  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out, String customerId) {
    ReportPoint point = FORMAT.drive(msg, defaultHostName, customerId, new ArrayList<>());
    if (point != null) {
      out.add(point);
    }
  }

  @Override
  public void decodeReportPoints(String msg, List<ReportPoint> out) {
    logger.log(Level.WARNING, "This decoder does not support customerId extraction, ignoring " + msg);
  }
}
