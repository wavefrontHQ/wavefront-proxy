package com.wavefront.ingester;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.regex.Pattern;

import sunnylabs.report.ReportPoint;

/**
 * Graphite decoder that takes in a point of the type:
 *
 * [metric] [value] [timestamp] [annotations]
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public class GraphiteDecoder implements Decoder {

  private static final Pattern CUSTOMERID = Pattern.compile("[a-z]+");
  private static final IngesterFormatter FORMAT = IngesterFormatter.newBuilder().whiteSpace()
      .appendMetricName().whiteSpace()
      .appendValue().whiteSpace()
      .appendOptionalTimestamp().whiteSpace()
      .appendAnnotationsConsumer().whiteSpace().build();
  private final String hostName;
  private List<String> customSourceTags;

  public GraphiteDecoder(List<String> customSourceTags) {
    this.hostName = "unknown";
    Preconditions.checkNotNull(customSourceTags);
    this.customSourceTags = customSourceTags;
  }

  public GraphiteDecoder(String hostName, List<String> customSourceTags) {
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
    List<ReportPoint> output = Lists.newArrayList();
    decodeReportPoints(msg, output, "dummy");
    if (!output.isEmpty()) {
      for (ReportPoint rp : output) {
        String metricName = rp.getMetric();
        List<String> metricParts = Lists.newArrayList(Splitter.on(".").split(metricName));
        if (metricParts.size() <= 1) {
          throw new RuntimeException("Metric name does not contain a customer id: " + metricName);
        }
        String customerId = metricParts.get(0);
        if (CUSTOMERID.matcher(customerId).matches()) {
          metricName = Joiner.on(".").join(metricParts.subList(1, metricParts.size()));
        }
        out.add(ReportPoint.newBuilder(rp).setMetric(metricName).setTable(customerId).build());
      }
    }
  }
}
