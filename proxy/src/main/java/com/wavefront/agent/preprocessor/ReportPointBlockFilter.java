package com.wavefront.agent.preprocessor;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import wavefront.report.ReportPoint;

/**
 * Blocking regex-based filter. Rejects a point if a specified component (metric, source, or point
 * tag value, depending on the "scope" parameter) doesn't match the regex.
 *
 * <p>Created by Vasily on 9/13/16.
 */
public class ReportPointBlockFilter implements AnnotatedPredicate<ReportPoint> {

  private final String scope;
  private final Pattern compiledPattern;
  private final PreprocessorRuleMetrics ruleMetrics;
  private final Predicate<ReportPoint> v2Predicate;
  private boolean isV1PredicatePresent = false;

  public ReportPointBlockFilter(
      final String scope,
      final String patternMatch,
      @Nullable final Predicate<ReportPoint> v2Predicate,
      final PreprocessorRuleMetrics ruleMetrics) {

    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
    // If v2 predicate is null, v1 predicate becomes mandatory.
    // v1 predicates = [scope, match]
    if (v2Predicate == null) {
      Preconditions.checkNotNull(scope, "[scope] can't be null");
      Preconditions.checkArgument(!scope.isEmpty(), "[scope] can't be blank");
      Preconditions.checkNotNull(patternMatch, "[match] can't be null");
      Preconditions.checkArgument(!patternMatch.isEmpty(), "[match] can't be blank");
      isV1PredicatePresent = true;
    } else {
      // If v2 predicate is present, verify all or none of v1 predicate parameters are present.
      boolean bothV1PredicatesValid =
          !Strings.isNullOrEmpty(scope) && !Strings.isNullOrEmpty(patternMatch);
      boolean bothV1PredicatesNull = scope == null && patternMatch == null;

      if (bothV1PredicatesValid) {
        isV1PredicatePresent = true;
      } else if (!bothV1PredicatesNull) {
        // Specifying any one of the v1Predicates and leaving it blank in considered invalid.
        throw new IllegalArgumentException(
            "[match], [scope] for rule should both be valid non "
                + "null/blank values or both null.");
      }
    }

    if (isV1PredicatePresent) {
      this.compiledPattern = Pattern.compile(patternMatch);
      this.scope = scope;
    } else {
      this.compiledPattern = null;
      this.scope = null;
    }
    this.v2Predicate = v2Predicate != null ? v2Predicate : x -> true;
  }

  @Override
  public boolean test(@Nonnull ReportPoint reportPoint, @Nullable String[] messageHolder) {
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (!v2Predicate.test(reportPoint)) return true;

      if (!isV1PredicatePresent) {
        ruleMetrics.incrementRuleAppliedCounter();
        return false;
      }

      // evaluates v1 predicate if present.
      switch (scope) {
        case "metricName":
          if (compiledPattern.matcher(reportPoint.getMetric()).matches()) {
            ruleMetrics.incrementRuleAppliedCounter();
            return false;
          }
          break;
        case "sourceName":
          if (compiledPattern.matcher(reportPoint.getHost()).matches()) {
            ruleMetrics.incrementRuleAppliedCounter();
            return false;
          }
          break;
        default:
          if (reportPoint.getAnnotations() != null) {
            String tagValue = reportPoint.getAnnotations().get(scope);
            if (tagValue != null) {
              if (compiledPattern.matcher(tagValue).matches()) {
                ruleMetrics.incrementRuleAppliedCounter();
                return false;
              }
            }
          }
      }
      return true;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
