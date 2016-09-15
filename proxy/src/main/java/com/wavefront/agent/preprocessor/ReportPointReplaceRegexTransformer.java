package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import com.yammer.metrics.core.Counter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import sunnylabs.report.ReportPoint;

/**
 * Created by Vasily on 9/13/16.
 */
public class ReportPointReplaceRegexTransformer implements Function<ReportPoint, ReportPoint> {

  private final String patternReplace;
  private final String scope;
  private final Pattern compiledPattern;
  private final Counter ruleAppliedCounter;

  public ReportPointReplaceRegexTransformer(final String patternMatch,
                                            final String patternReplace,
                                            final String scope,
                                            @Nullable final Counter ruleAppliedCounter)
  {
    Preconditions.checkNotNull(scope);
    Preconditions.checkNotNull(patternMatch);
    Preconditions.checkNotNull(patternReplace);
    Preconditions.checkArgument(!patternMatch.isEmpty());
    this.patternReplace = patternReplace;
    this.scope = scope;
    this.compiledPattern = Pattern.compile(patternMatch);
    this.ruleAppliedCounter = ruleAppliedCounter;
  }

  @Override
  public ReportPoint apply(ReportPoint reportPoint) {
    Matcher patternMatcher;
    switch (scope) {
      case "metricName":
        patternMatcher = compiledPattern.matcher(reportPoint.getMetric());
        if (patternMatcher.matches()) {
          reportPoint.setMetric(patternMatcher.replaceAll(patternReplace));
          if (ruleAppliedCounter != null) {
            ruleAppliedCounter.inc();
          }
        }
        break;
      case "sourceName":
        patternMatcher = compiledPattern.matcher(reportPoint.getHost());
        if (patternMatcher.matches()) {
          reportPoint.setHost(patternMatcher.replaceAll(patternReplace));
          if (ruleAppliedCounter != null) {
            ruleAppliedCounter.inc();
          }
        }
        break;
      default:
        String tagValue = reportPoint.getAnnotations().get(scope);
        if (tagValue != null) {
          patternMatcher = compiledPattern.matcher(tagValue);
          if (patternMatcher.matches()) {
            reportPoint.getAnnotations().put(scope, patternMatcher.replaceAll(patternReplace));
            if (ruleAppliedCounter != null) {
              ruleAppliedCounter.inc();
            }
          }
        }
    }
    return reportPoint;
  }
}
