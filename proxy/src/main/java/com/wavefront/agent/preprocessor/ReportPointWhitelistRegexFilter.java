package com.wavefront.agent.preprocessor;

import com.google.common.base.Preconditions;

import com.yammer.metrics.core.Counter;

import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import wavefront.report.ReportPoint;

/**
 * Whitelist regex filter. Rejects a point if a specified component (metric, source, or point tag value, depending
 * on the "scope" parameter) doesn't match the regex.
 *
 * Created by Vasily on 9/13/16.
 */
public class ReportPointWhitelistRegexFilter extends AnnotatedPredicate<ReportPoint> {

  private final String scope;
  private final Pattern compiledPattern;
  @Nullable
  private final Counter ruleAppliedCounter;

  public ReportPointWhitelistRegexFilter(final String scope,
                                         final String patternMatch,
                                         @Nullable final Counter ruleAppliedCounter) {
    this.compiledPattern = Pattern.compile(Preconditions.checkNotNull(patternMatch, "[match] can't be null"));
    Preconditions.checkArgument(!patternMatch.isEmpty(), "[match] can't be blank");
    this.scope = Preconditions.checkNotNull(scope, "[scope] can't be null");
    Preconditions.checkArgument(!scope.isEmpty(), "[scope] can't be blank");
    this.ruleAppliedCounter = ruleAppliedCounter;
  }

  @Override
  public boolean apply(@NotNull ReportPoint reportPoint) {
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
        if (reportPoint.getAnnotations() != null) {
          String tagValue = reportPoint.getAnnotations().get(scope);
          if (tagValue != null && compiledPattern.matcher(tagValue).matches()) {
            return true;
          }
        }
        if (ruleAppliedCounter != null) {
          ruleAppliedCounter.inc();
        }
        return false;
    }
    return true;
  }
}
