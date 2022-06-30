package com.wavefront.agent.preprocessor;

import static com.wavefront.agent.preprocessor.PreprocessorUtil.truncate;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import wavefront.report.Annotation;
import wavefront.report.ReportLog;

public class ReportLogLimitLengthTransformer implements Function<ReportLog, ReportLog> {

  private final String scope;
  private final int maxLength;
  private final LengthLimitActionType actionSubtype;
  @Nullable private final Pattern compiledMatchPattern;
  private final Predicate<ReportLog> v2Predicate;

  private final PreprocessorRuleMetrics ruleMetrics;

  public ReportLogLimitLengthTransformer(
      @Nonnull final String scope,
      final int maxLength,
      @Nonnull final LengthLimitActionType actionSubtype,
      @Nullable final String patternMatch,
      @Nullable final Predicate<ReportLog> v2Predicate,
      @Nonnull final PreprocessorRuleMetrics ruleMetrics) {
    this.scope = Preconditions.checkNotNull(scope, "[scope] can't be null");
    Preconditions.checkArgument(!scope.isEmpty(), "[scope] can't be blank");
    if (actionSubtype == LengthLimitActionType.DROP
        && (scope.equals("message") || scope.equals("sourceName"))) {
      throw new IllegalArgumentException(
          "'drop' action type can't be used in message and sourceName scope!");
    }
    if (actionSubtype == LengthLimitActionType.TRUNCATE_WITH_ELLIPSIS && maxLength < 3) {
      throw new IllegalArgumentException(
          "'maxLength' must be at least 3 for 'truncateWithEllipsis' action type!");
    }
    Preconditions.checkArgument(maxLength > 0, "[maxLength] needs to be > 0!");
    this.maxLength = maxLength;
    this.actionSubtype = actionSubtype;
    this.compiledMatchPattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    this.ruleMetrics = ruleMetrics;
    this.v2Predicate = v2Predicate != null ? v2Predicate : x -> true;
  }

  @Nullable
  @Override
  public ReportLog apply(@Nullable ReportLog reportLog) {
    if (reportLog == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (!v2Predicate.test(reportLog)) return reportLog;

      switch (scope) {
        case "message":
          if (reportLog.getMessage().length() > maxLength
              && (compiledMatchPattern == null
                  || compiledMatchPattern.matcher(reportLog.getMessage()).matches())) {
            reportLog.setMessage(truncate(reportLog.getMessage(), maxLength, actionSubtype));
            ruleMetrics.incrementRuleAppliedCounter();
          }
          break;
        case "sourceName":
          if (reportLog.getHost().length() > maxLength
              && (compiledMatchPattern == null
                  || compiledMatchPattern.matcher(reportLog.getHost()).matches())) {
            reportLog.setHost(truncate(reportLog.getHost(), maxLength, actionSubtype));
            ruleMetrics.incrementRuleAppliedCounter();
          }
          break;
        default:
          List<Annotation> annotations = new ArrayList<>(reportLog.getAnnotations());
          Iterator<Annotation> iterator = annotations.iterator();
          boolean changed = false;
          while (iterator.hasNext()) {
            Annotation entry = iterator.next();
            if (entry.getKey().equals(scope) && entry.getValue().length() > maxLength) {
              if (compiledMatchPattern == null
                  || compiledMatchPattern.matcher(entry.getValue()).matches()) {
                changed = true;
                if (actionSubtype == LengthLimitActionType.DROP) {
                  iterator.remove();
                } else {
                  entry.setValue(truncate(entry.getValue(), maxLength, actionSubtype));
                }
                ruleMetrics.incrementRuleAppliedCounter();
              }
            }
          }
          if (changed) {
            reportLog.setAnnotations(annotations);
          }
      }
      return reportLog;

    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
