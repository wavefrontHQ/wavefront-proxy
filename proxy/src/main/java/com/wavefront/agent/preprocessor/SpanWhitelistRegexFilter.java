package com.wavefront.agent.preprocessor;

import com.google.common.base.Preconditions;

import java.util.regex.Pattern;

import javax.annotation.Nonnull;

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

  public SpanWhitelistRegexFilter(final String scope,
                                         final String patternMatch,
                                         final PreprocessorRuleMetrics ruleMetrics) {
    this.compiledPattern = Pattern.compile(Preconditions.checkNotNull(patternMatch,
        "[match] can't be null"));
    Preconditions.checkArgument(!patternMatch.isEmpty(), "[match] can't be blank");
    this.scope = Preconditions.checkNotNull(scope, "[scope] can't be null");
    Preconditions.checkArgument(!scope.isEmpty(), "[scope] can't be blank");
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
  }

  @Override
  public boolean test(@Nonnull Span span, String[] messageHolder) {
    long startNanos = ruleMetrics.ruleStart();
    try {
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
