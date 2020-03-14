package com.wavefront.agent.preprocessor;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import wavefront.report.Annotation;
import wavefront.report.Span;

/**
 * Whitelist regex filter. Rejects a span if a specified component (name, source, or
 * annotation value, depending on the "scope" parameter) doesn't match the regex.
 *
 * @author vasily@wavefront.com
 */
public class SpanWhitelistRegexFilter implements AnnotatedPredicate<Span> {

  private final String scope;
  private final Pattern compiledPattern;
  private final PreprocessorRuleMetrics ruleMetrics;
  @Nullable
  private final Map<String, Object> v2Predicate;
  private boolean isV1PredicatePresent = false;

  public SpanWhitelistRegexFilter(final String scope,
                                  final String patternMatch,
                                  @Nullable final Map<String, Object> v2Predicate,
                                  final PreprocessorRuleMetrics ruleMetrics) {
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
    this.v2Predicate = v2Predicate;

    // If v2 predicate is null, v1 predicate becomes mandatory.
    // v1 predicates = [scope, match]
    if (v2Predicate == null || v2Predicate.isEmpty()) {
      Preconditions.checkNotNull(scope, "[scope] can't be null");
      Preconditions.checkArgument(!scope.isEmpty(), "[scope] can't be blank");
      Preconditions.checkNotNull(patternMatch, "[match] can't be null");
      Preconditions.checkArgument(!patternMatch.isEmpty(), "[match] can't be blank");
      isV1PredicatePresent = true;
    } else {
      // If v2 predicate is present, verify all or none of v1 predicate parameters are present.
      boolean bothV1PredicatesValid = !Strings.isNullOrEmpty(scope) && !Strings.isNullOrEmpty(patternMatch);
      boolean bothV1PredicatesNull = scope == null && patternMatch == null;

      if (bothV1PredicatesValid) {
        isV1PredicatePresent = true;
      } else if (!bothV1PredicatesNull) {
        // Specifying any one of the v1Predicates and leaving it blank in considered invalid.
        Preconditions.checkArgument(false, "[match], [scope] for rule should both be valid non " +
            "null/blank values or both null.");
      }
    }

    if(isV1PredicatePresent) {
      this.compiledPattern = Pattern.compile(patternMatch);
      this.scope = scope;
    } else {
      this.compiledPattern = null;
      this.scope = null;
    }
  }

  @Override
  public boolean test(@Nonnull Span span, String[] messageHolder) {
    long startNanos = ruleMetrics.ruleStart();
    try {
      // Test for preprocessor v2 predicate.
      if (!PreprocessorUtil.isRuleApplicable(v2Predicate, span)) return false;
      if (!isV1PredicatePresent) {
        ruleMetrics.incrementRuleAppliedCounter();
        return true;
      }

      // Evaluate v1 predicate if present.
      switch (scope) {
        case "spanName":
          if (!compiledPattern.matcher(span.getName()).matches()) {
            ruleMetrics.incrementRuleAppliedCounter();
            return false;
          }
          break;
        case "sourceName":
          if (!compiledPattern.matcher(span.getSource()).matches()) {
            ruleMetrics.incrementRuleAppliedCounter();
            return false;
          }
          break;
        default:
          for (Annotation annotation : span.getAnnotations()) {
            if (annotation.getKey().equals(scope) &&
                compiledPattern.matcher(annotation.getValue()).matches()) {
              return true;
            }
          }
          ruleMetrics.incrementRuleAppliedCounter();
          return false;
      }
      return true;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
