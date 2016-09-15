package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import com.yammer.metrics.core.Counter;

import java.util.regex.Pattern;

import javax.annotation.Nullable;

import sunnylabs.report.ReportPoint;

/**
 * Created by Vasily on 9/13/16.
 */
public class ReportPointRenameTagTransformer implements Function<ReportPoint, ReportPoint> {
  private final String patternReplace;
  private final String scope;
  private final Pattern compiledPattern;
  private final Counter ruleAppliedCounter;

  public ReportPointRenameTagTransformer(@Nullable final String patternMatch,
                                         final String target,
                                         final String scope,
                                         @Nullable final Counter ruleAppliedCounter)
  {
    Preconditions.checkNotNull(scope);
    Preconditions.checkNotNull(target);
    Preconditions.checkArgument(!target.isEmpty());
    this.patternReplace = target;
    this.scope = scope;
    if (patternMatch != null) {
      this.compiledPattern = Pattern.compile(patternMatch);
    } else {
      this.compiledPattern = null;
    }
    this.ruleAppliedCounter = ruleAppliedCounter;
  }

  @Override
  public ReportPoint apply(ReportPoint reportPoint) {
    String tagValue = reportPoint.getAnnotations().get(scope);
    if (tagValue == null) {
      return reportPoint;
    }
    if (compiledPattern != null && !compiledPattern.matcher(tagValue).matches()) {
      return reportPoint;
    }
    reportPoint.getAnnotations().remove(scope);
    reportPoint.getAnnotations().put(patternReplace, tagValue);
    if (ruleAppliedCounter != null) {
      ruleAppliedCounter.inc();
    }
    return reportPoint;
  }
}
