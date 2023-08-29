package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import javax.annotation.Nullable;
import wavefront.report.ReportPoint;

/**
 * Add prefix transformer. Add a metric name prefix, if defined, to all points.
 *
 * <p>Created by Vasily on 9/15/16.
 */
public class ReportPointAddPrefixTransformer implements Function<ReportPoint, ReportPoint> {

  @Nullable private final String prefix;

  public ReportPointAddPrefixTransformer(@Nullable final String prefix) {
    this.prefix = prefix;
  }

  @Nullable
  @Override
  public ReportPoint apply(@Nullable ReportPoint reportPoint) {
    if (reportPoint == null) return null;
    String metric = reportPoint.getMetric();
    boolean isTildaPrefixed = metric.charAt(0) == 126;
    boolean isDeltaPrefixed = (metric.charAt(0) == 0x2206) || (metric.charAt(0) == 0x0394);
    boolean isDeltaTildaPrefixed = isDeltaPrefixed && metric.charAt(1) == 126;
    // only append prefix if metric does not begin with tilda, delta or delta tilda prefix
    if (prefix != null && !prefix.isEmpty()
        && !(isTildaPrefixed || isDeltaTildaPrefixed || isDeltaPrefixed)) {
      reportPoint.setMetric(prefix + "." + metric);
    }
    return reportPoint;
  }
}
