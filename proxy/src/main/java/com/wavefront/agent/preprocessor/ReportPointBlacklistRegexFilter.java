package com.wavefront.agent.preprocessor;

import com.google.common.base.Preconditions;

import com.yammer.metrics.core.Counter;

import java.util.regex.Pattern;

import javax.annotation.Nullable;

import sunnylabs.report.ReportPoint;

/**
 * Blacklist regex filter. Rejects a point if a specified component (metric, source, or point tag value, depending
 * on the "scope" parameter) doesn't match the regex.
 *
 * Created by Vasily on 9/13/16.
 */
public class ReportPointBlacklistRegexFilter extends AnnotatedPredicate<ReportPoint> {

  private final String scope;
  private final Pattern compiledPattern;
  @Nullable
  private final Counter ruleAppliedCounter;

  public ReportPointBlacklistRegexFilter(final String scope,
                                         final String patternMatch,
                                         @Nullable final Counter ruleAppliedCounter) {
    Preconditions.checkNotNull(patternMatch, "[match] can't be null");
    Preconditions.checkNotNull(scope, "[scope] can't be null");
    this.scope = scope;
    this.compiledPattern = Pattern.compile(patternMatch);
    this.ruleAppliedCounter = ruleAppliedCounter;
  }

  @Override
  public boolean apply(ReportPoint reportPoint) {
    switch (scope) {
      case "metricName":
        if (compiledPattern.matcher(reportPoint.getMetric()).matches()) {
          if (ruleAppliedCounter != null) {
            ruleAppliedCounter.inc();
          }
          return false;
        }
        break;
      case "sourceName":
        if (compiledPattern.matcher(reportPoint.getHost()).matches()) {
          if (ruleAppliedCounter != null) {
            ruleAppliedCounter.inc();
          }
          return false;
        }
        break;
      default:
        if (reportPoint.getAnnotations() != null) {
          String tagValue = reportPoint.getAnnotations().get(scope);
          if (tagValue != null) {
            if (compiledPattern.matcher(tagValue).matches()) {
              if (ruleAppliedCounter != null) {
                ruleAppliedCounter.inc();
              }
              return false;
            }
          }
        }
    }
    return true;
  }
}
