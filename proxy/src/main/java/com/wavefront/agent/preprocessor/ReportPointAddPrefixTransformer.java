package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;

import javax.annotation.Nullable;

import wavefront.report.ReportPoint;

/**
 * Add prefix transformer. Add a metric name prefix, if defined, to all points.
 *
 * Created by Vasily on 9/15/16.
 */
public class ReportPointAddPrefixTransformer implements Function<ReportPoint, ReportPoint> {

  @Nullable
  private final String prefix;

  public ReportPointAddPrefixTransformer(@Nullable final String prefix) {
    this.prefix = prefix;
  }

  @Nullable
  @Override
  public ReportPoint apply(@Nullable ReportPoint reportPoint) {
    if (reportPoint == null) return null;
    if (prefix != null && !prefix.isEmpty()) {
      reportPoint.setMetric(prefix + "." + reportPoint.getMetric());
    }
    return reportPoint;
  }
}
