package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import wavefront.report.ReportPoint;

import static com.wavefront.agent.preprocessor.PreprocessorUtil.truncate;

public class ReportPointLimitLengthTransformer implements Function<ReportPoint, ReportPoint> {

  private final String scope;
  private final int maxLength;
  private final LengthLimitActionType actionSubtype;
  @Nullable
  private final Pattern compiledMatchPattern;
  @Nullable
  private final Map<String, Object> v2Predicate;

  private final PreprocessorRuleMetrics ruleMetrics;

  public ReportPointLimitLengthTransformer(@Nonnull final String scope,
                                           final int maxLength,
                                           @Nonnull final LengthLimitActionType actionSubtype,
                                           @Nullable final String patternMatch,
                                           @Nullable final Map<String, Object> v2Predicate,
                                           @Nonnull final PreprocessorRuleMetrics ruleMetrics) {
    this.scope = Preconditions.checkNotNull(scope, "[scope] can't be null");
    Preconditions.checkArgument(!scope.isEmpty(), "[scope] can't be blank");
    if (actionSubtype == LengthLimitActionType.DROP && (scope.equals("metricName") || scope.equals("sourceName"))) {
      throw new IllegalArgumentException("'drop' action type can't be used in metricName and sourceName scope!");
    }
    if (actionSubtype == LengthLimitActionType.TRUNCATE_WITH_ELLIPSIS && maxLength < 3) {
      throw new IllegalArgumentException("'maxLength' must be at least 3 for 'truncateWithEllipsis' action type!");
    }
    Preconditions.checkArgument(maxLength > 0, "[maxLength] needs to be > 0!");
    this.maxLength = maxLength;
    this.actionSubtype = actionSubtype;
    this.compiledMatchPattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    this.ruleMetrics = ruleMetrics;
    this.v2Predicate = v2Predicate;
  }

  @Nullable
  @Override
  public ReportPoint apply(@Nullable ReportPoint reportPoint) {
    if (reportPoint == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    // Test for preprocessor v2 predicate.
    if (!PreprocessorUtil.isRuleApplicable(v2Predicate, reportPoint)) return reportPoint;

    switch (scope) {
      case "metricName":
        if (reportPoint.getMetric().length() > maxLength && (compiledMatchPattern == null ||
            compiledMatchPattern.matcher(reportPoint.getMetric()).matches())) {
          reportPoint.setMetric(truncate(reportPoint.getMetric(), maxLength, actionSubtype));
          ruleMetrics.incrementRuleAppliedCounter();
        }
        break;
      case "sourceName":
        if (reportPoint.getHost().length() > maxLength && (compiledMatchPattern == null ||
            compiledMatchPattern.matcher(reportPoint.getHost()).matches())) {
          reportPoint.setHost(truncate(reportPoint.getHost(), maxLength, actionSubtype));
          ruleMetrics.incrementRuleAppliedCounter();
        }
        break;
      default:
        if (reportPoint.getAnnotations() != null) {
          String tagValue = reportPoint.getAnnotations().get(scope);
          if (tagValue != null && tagValue.length() > maxLength) {
            if (actionSubtype == LengthLimitActionType.DROP) {
              reportPoint.getAnnotations().remove(scope);
              ruleMetrics.incrementRuleAppliedCounter();
            } else {
              if (compiledMatchPattern == null || compiledMatchPattern.matcher(tagValue).matches()) {
                reportPoint.getAnnotations().put(scope, truncate(tagValue, maxLength,
                    actionSubtype));
                ruleMetrics.incrementRuleAppliedCounter();
              }
            }
          }
        }
    }
    ruleMetrics.ruleEnd(startNanos);
    return reportPoint;
  }
}
