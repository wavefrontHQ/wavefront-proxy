package com.wavefront.agent.preprocessor;

import com.google.common.base.Preconditions;

import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.annotation.Nonnull;

import wavefront.report.ReportPoint;

/**
 * Blacklist regex filter. Rejects a point if a specified component (metric, source, or point tag value, depending
 * on the "scope" parameter) doesn't match the regex.
 *
 * Created by Vasily on 9/13/16.
 */
public class ReportPointBlacklistRegexFilter implements AnnotatedPredicate<ReportPoint> {

  private final String scope;
  private final Pattern compiledPattern;
  private final PreprocessorRuleMetrics ruleMetrics;

  public ReportPointBlacklistRegexFilter(final String scope,
                                         final String patternMatch,
                                         final PreprocessorRuleMetrics ruleMetrics) {
    this.compiledPattern = Pattern.compile(Preconditions.checkNotNull(patternMatch, "[match] can't be null"));
    Preconditions.checkArgument(!patternMatch.isEmpty(), "[match] can't be blank");
    this.scope = Preconditions.checkNotNull(scope, "[scope] can't be null");
    Preconditions.checkArgument(!scope.isEmpty(), "[scope] can't be blank");
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
  }

  @Override
  public boolean test(@Nonnull ReportPoint reportPoint, @Nullable String[] messageHolder) {
    long startNanos = ruleMetrics.ruleStart();
    switch (scope) {
      case "metricName":
        if (compiledPattern.matcher(reportPoint.getMetric()).matches()) {
          ruleMetrics.incrementRuleAppliedCounter();
          ruleMetrics.ruleEnd(startNanos);
          return false;
        }
        break;
      case "sourceName":
        if (compiledPattern.matcher(reportPoint.getHost()).matches()) {
          ruleMetrics.incrementRuleAppliedCounter();
          ruleMetrics.ruleEnd(startNanos);
          return false;
        }
        break;
      default:
        if (reportPoint.getAnnotations() != null) {
          String tagValue = reportPoint.getAnnotations().get(scope);
          if (tagValue != null) {
            if (compiledPattern.matcher(tagValue).matches()) {
              ruleMetrics.incrementRuleAppliedCounter();
              ruleMetrics.ruleEnd(startNanos);
              return false;
            }
          }
        }
    }
    ruleMetrics.ruleEnd(startNanos);
    return true;
  }
}
