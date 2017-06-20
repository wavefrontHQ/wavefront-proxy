package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

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

  @Override
  public ReportPoint apply(@NotNull ReportPoint reportPoint) {
    if (prefix != null && !prefix.isEmpty()) {
      reportPoint.setMetric(prefix + "." + reportPoint.getMetric());
    }
    return reportPoint;
  }
}
