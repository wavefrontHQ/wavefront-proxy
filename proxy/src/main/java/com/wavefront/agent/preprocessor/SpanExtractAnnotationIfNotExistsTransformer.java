package com.wavefront.agent.preprocessor;

import java.util.Map;

import javax.annotation.Nullable;

import wavefront.report.Span;

/**
 * Create a new span annotation by extracting a portion of a span name, source name or another annotation
 *
 * @author vasily@wavefront.com
 */
public class SpanExtractAnnotationIfNotExistsTransformer extends SpanExtractAnnotationTransformer {

  @Nullable
  private final Map<String, Object> v2Predicate;

  public SpanExtractAnnotationIfNotExistsTransformer(final String key,
                                                     final String input,
                                                     final String patternSearch,
                                                     final String patternReplace,
                                                     @Nullable final String replaceInput,
                                                     @Nullable final String patternMatch,
                                                     final boolean firstMatchOnly,
                                                     @Nullable final Map<String, Object> v2Predicate,
                                                     final PreprocessorRuleMetrics ruleMetrics) {
    super(key, input, patternSearch, patternReplace, replaceInput, patternMatch, firstMatchOnly,
        v2Predicate, ruleMetrics);
    this.v2Predicate = v2Predicate;
  }

  @Nullable
  @Override
  public Span apply(@Nullable Span span) {
    if (span == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    if (!PreprocessorUtil.isRuleApplicable(v2Predicate, span)) return span;

    if (span.getAnnotations().stream().noneMatch(a -> a.getKey().equals(key))) {
      internalApply(span);
    }
    ruleMetrics.ruleEnd(startNanos);
    return span;
  }
}
