package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import com.yammer.metrics.core.Counter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import sunnylabs.report.ReportPoint;

/**
 * Replace regex transformer. Performs search and replace on a specified component of a point (metric name,
 * source name or a point tag value, depending on "scope" parameter.
 *
 * Created by Vasily on 9/13/16.
 */
public class ReportPointReplaceRegexTransformer implements Function<ReportPoint, ReportPoint> {

  private final String patternReplace;
  private final String scope;
  private final Pattern compiledPattern;
  @Nullable
  private final Counter ruleAppliedCounter;

  public ReportPointReplaceRegexTransformer(final String scope,
                                            final String patternMatch,
                                            final String patternReplace,
                                            @Nullable final Counter ruleAppliedCounter) {
    this.compiledPattern = Pattern.compile(Preconditions.checkNotNull(patternMatch, "[match] can't be null"));
    Preconditions.checkArgument(!patternMatch.isEmpty(), "[match] can't be blank");
    this.scope = Preconditions.checkNotNull(scope, "[scope] can't be null");
    Preconditions.checkArgument(!scope.isEmpty(), "[scope] can't be blank");
    this.patternReplace = Preconditions.checkNotNull(patternReplace, "[replace] can't be null");
    this.ruleAppliedCounter = ruleAppliedCounter;
  }

  @Override
  public ReportPoint apply(@NotNull ReportPoint reportPoint) {
    Matcher patternMatcher;
    switch (scope) {
      case "metricName":
        patternMatcher = compiledPattern.matcher(reportPoint.getMetric());
        if (patternMatcher.find()) {
          reportPoint.setMetric(patternMatcher.replaceAll(patternReplace));
          if (ruleAppliedCounter != null) {
            ruleAppliedCounter.inc();
          }
        }
        break;
      case "sourceName":
        patternMatcher = compiledPattern.matcher(reportPoint.getHost());
        if (patternMatcher.find()) {
          reportPoint.setHost(patternMatcher.replaceAll(patternReplace));
          if (ruleAppliedCounter != null) {
            ruleAppliedCounter.inc();
          }
        }
        break;
      default:
        if (reportPoint.getAnnotations() != null) {
          String tagValue = reportPoint.getAnnotations().get(scope);
          if (tagValue != null) {
            patternMatcher = compiledPattern.matcher(tagValue);
            if (patternMatcher.find()) {
              reportPoint.getAnnotations().put(scope, patternMatcher.replaceAll(patternReplace));
              if (ruleAppliedCounter != null) {
                ruleAppliedCounter.inc();
              }
            }
          }
        }
    }
    return reportPoint;
  }
}
