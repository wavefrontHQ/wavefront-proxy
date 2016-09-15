package com.wavefront.agent.preprocessor;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

import com.yammer.metrics.core.Counter;

import java.util.regex.Pattern;

import javax.annotation.Nullable;

import sunnylabs.report.ReportPoint;

/**
 * Created by Vasily on 9/13/16.
 */
public class ReportPointWhitelistRegexFilter implements Predicate<ReportPoint> {
  private final String scope;
  private final Pattern compiledPattern;
  private final Counter ruleAppliedCounter;

  public ReportPointWhitelistRegexFilter(final String patternMatch,
                                         final String scope,
                                         @Nullable final Counter ruleAppliedCounter) {
    Preconditions.checkNotNull(patternMatch);
    Preconditions.checkNotNull(scope);
    this.scope = scope;
    this.compiledPattern = Pattern.compile(patternMatch);
    this.ruleAppliedCounter = ruleAppliedCounter;
  }

  @Override
  public boolean apply(ReportPoint reportPoint) {
    switch (scope) {
      case "metricName":
        if (!compiledPattern.matcher(reportPoint.getMetric()).matches()) {
          if (ruleAppliedCounter != null) {
            ruleAppliedCounter.inc();
          }
          return false;
        }
        break;
      case "sourceName":
        if (!compiledPattern.matcher(reportPoint.getHost()).matches()) {
          if (ruleAppliedCounter != null) {
            ruleAppliedCounter.inc();
          }
          return false;
        }
        break;
      default:
        String tagValue = reportPoint.getAnnotations().get(scope);
        if (tagValue != null) {
          if (!compiledPattern.matcher(tagValue).matches()) {
            if (ruleAppliedCounter != null) {
              ruleAppliedCounter.inc();
            }
            return false;
          }
        }
    }
    return true;
  }
}
