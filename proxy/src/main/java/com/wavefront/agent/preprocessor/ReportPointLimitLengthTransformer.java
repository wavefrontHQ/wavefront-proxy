package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import wavefront.report.ReportPoint;

public class ReportPointLimitLengthTransformer implements Function<ReportPoint, ReportPoint> {

  private final String scope;
  private final int maxLength;
  private final LengthLimitActionType actionSubtype;
  @Nullable
  private final Pattern compiledMatchPattern;

  private final PreprocessorRuleMetrics ruleMetrics;

  public ReportPointLimitLengthTransformer(@Nonnull final String scope,
                                           final int maxLength,
                                           @Nonnull final LengthLimitActionType actionSubtype,
                                           @Nullable final String patternMatch,
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
  }

  private String truncate(String input) {
    if (input.length() > maxLength && (compiledMatchPattern == null || compiledMatchPattern.matcher(input).matches())) {
      ruleMetrics.incrementRuleAppliedCounter();
      switch (actionSubtype) {
        case TRUNCATE:
          return input.substring(0, maxLength);
        case TRUNCATE_WITH_ELLIPSIS:
          return input.substring(0, maxLength - 3) + "...";
        default:
          return input;
      }
    }
    return input;
  }

  public ReportPoint apply(@Nonnull ReportPoint reportPoint) {
    long startNanos = ruleMetrics.ruleStart();
    switch (scope) {
      case "metricName":
        reportPoint.setMetric(truncate(reportPoint.getMetric()));
        break;
      case "sourceName":
        reportPoint.setHost(truncate(reportPoint.getHost()));
        break;
      default:
        if (reportPoint.getAnnotations() != null) {
          String tagValue = reportPoint.getAnnotations().get(scope);
          if (tagValue != null) {
            if (actionSubtype == LengthLimitActionType.DROP && tagValue.length() > maxLength) {
              reportPoint.getAnnotations().remove(scope);
              ruleMetrics.incrementRuleAppliedCounter();
            } else {
              reportPoint.getAnnotations().put(scope, truncate(tagValue));
            }
          }
        }
    }
    ruleMetrics.ruleEnd(startNanos);
    return reportPoint;
  }
}
