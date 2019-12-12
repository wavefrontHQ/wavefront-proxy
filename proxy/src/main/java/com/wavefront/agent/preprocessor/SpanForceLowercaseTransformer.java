package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import java.util.regex.Pattern;

import javax.annotation.Nullable;

import wavefront.report.Annotation;
import wavefront.report.Span;

/**
 * Force lowercase transformer. Converts a specified component of a point (metric name, source name or a point tag
 * value, depending on "scope" parameter) to lower case to enforce consistency.
 *
 * @author vasily@wavefront.com
 */
public class SpanForceLowercaseTransformer implements Function<Span, Span> {

  private final String scope;
  @Nullable
  private final Pattern compiledMatchPattern;
  private final boolean firstMatchOnly;
  private final PreprocessorRuleMetrics ruleMetrics;

  public SpanForceLowercaseTransformer(final String scope,
                                       @Nullable final String patternMatch,
                                       final boolean firstMatchOnly,
                                       final PreprocessorRuleMetrics ruleMetrics) {
    this.scope = Preconditions.checkNotNull(scope, "[scope] can't be null");
    Preconditions.checkArgument(!scope.isEmpty(), "[scope] can't be blank");
    this.compiledMatchPattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    this.firstMatchOnly = firstMatchOnly;
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
  }

  @Nullable
  @Override
  public Span apply(@Nullable Span span) {
    if (span == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    switch (scope) {
      case "spanName":
        if (compiledMatchPattern != null && !compiledMatchPattern.matcher(span.getName()).matches()) {
          break;
        }
        span.setName(span.getName().toLowerCase());
        ruleMetrics.incrementRuleAppliedCounter();
        break;
      case "sourceName": // source name is not case sensitive in Wavefront, but we'll do it anyway
        if (compiledMatchPattern != null && !compiledMatchPattern.matcher(span.getSource()).matches()) {
          break;
        }
        span.setSource(span.getSource().toLowerCase());
        ruleMetrics.incrementRuleAppliedCounter();
        break;
      default:
        for (Annotation x : span.getAnnotations()) {
          if (x.getKey().equals(scope) && (compiledMatchPattern == null ||
              compiledMatchPattern.matcher(x.getValue()).matches())) {
            x.setValue(x.getValue().toLowerCase());
            ruleMetrics.incrementRuleAppliedCounter();
            if (firstMatchOnly) {
              break;
            }
          }
        }
    }
    ruleMetrics.ruleEnd(startNanos);
    return span;
  }
}
