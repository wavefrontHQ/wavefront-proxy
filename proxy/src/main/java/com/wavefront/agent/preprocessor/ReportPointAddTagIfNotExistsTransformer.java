package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import com.yammer.metrics.core.Counter;

import javax.annotation.Nullable;

import sunnylabs.report.ReportPoint;

/**
 * Created by Vasily on 9/13/16.
 */
public class ReportPointAddTagIfNotExistsTransformer implements Function<ReportPoint, ReportPoint> {
  private final String patternReplace;
  private final String scope;
  private final Counter ruleAppliedCounter;

  public ReportPointAddTagIfNotExistsTransformer(final String value,
                                                 final String scope,
                                                 @Nullable final Counter ruleAppliedCounter)
  {
    Preconditions.checkNotNull(scope);
    Preconditions.checkNotNull(value);
    Preconditions.checkArgument(!value.isEmpty());
    this.patternReplace = value;
    this.scope = scope;
    this.ruleAppliedCounter = ruleAppliedCounter;
  }

  @Override
  public ReportPoint apply(ReportPoint reportPoint) {
    if (reportPoint.getAnnotations().get(scope) == null) {
      reportPoint.getAnnotations().put(scope, patternReplace);
      if (ruleAppliedCounter != null) {
        ruleAppliedCounter.inc();
      }
    }
    return reportPoint;
  }
}
