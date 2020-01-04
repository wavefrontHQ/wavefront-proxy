package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import com.yammer.metrics.core.Counter;

import java.util.regex.Pattern;

import javax.annotation.Nullable;

import wavefront.report.ReportPoint;

/**
 * Force lowercase transformer. Converts a specified component of a point (metric name,
 * source name or a point tag value, depending on "scope" parameter) to lower case to
 * enforce consistency.
 *
 * @author vasily@wavefront.com
 */
public class ReportPointForceLowercaseTransformer implements Function<ReportPoint, ReportPoint> {

  private final String scope;
  @Nullable
  private final Pattern compiledMatchPattern;
  private final PreprocessorRuleMetrics ruleMetrics;

  public ReportPointForceLowercaseTransformer(final String scope,
                                              @Nullable final String patternMatch,
                                              final PreprocessorRuleMetrics ruleMetrics) {
    this.scope = Preconditions.checkNotNull(scope, "[scope] can't be null");
    Preconditions.checkArgument(!scope.isEmpty(), "[scope] can't be blank");
    this.compiledMatchPattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
  }

  @Nullable
  @Override
  public ReportPoint apply(@Nullable ReportPoint reportPoint) {
    if (reportPoint == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    switch (scope) {
      case "metricName":
        if (compiledMatchPattern != null && !compiledMatchPattern.matcher(
            reportPoint.getMetric()).matches()) {
          break;
        }
        reportPoint.setMetric(reportPoint.getMetric().toLowerCase());
        ruleMetrics.incrementRuleAppliedCounter();
        break;
      case "sourceName": // source name is not case sensitive in Wavefront, but we'll do it anyway
        if (compiledMatchPattern != null && !compiledMatchPattern.matcher(
            reportPoint.getHost()).matches()) {
          break;
        }
        reportPoint.setHost(reportPoint.getHost().toLowerCase());
        ruleMetrics.incrementRuleAppliedCounter();
        break;
      default:
        if (reportPoint.getAnnotations() != null) {
          String tagValue = reportPoint.getAnnotations().get(scope);
          if (tagValue != null) {
            if (compiledMatchPattern != null && !compiledMatchPattern.matcher(tagValue).matches()) {
              break;
            }
            reportPoint.getAnnotations().put(scope, tagValue.toLowerCase());
            ruleMetrics.incrementRuleAppliedCounter();
          }
        }
    }
    ruleMetrics.ruleEnd(startNanos);
    return reportPoint;
  }
}
