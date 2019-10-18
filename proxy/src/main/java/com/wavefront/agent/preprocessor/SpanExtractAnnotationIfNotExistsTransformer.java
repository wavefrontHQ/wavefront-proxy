package com.wavefront.agent.preprocessor;

import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import wavefront.report.Span;

/**
 * Create a new span annotation by extracting a portion of a span name, source name or another annotation
 *
 * @author vasily@wavefront.com
 */
public class SpanExtractAnnotationIfNotExistsTransformer extends SpanExtractAnnotationTransformer {

  public SpanExtractAnnotationIfNotExistsTransformer(final String key,
                                                     final String input,
                                                     final String patternSearch,
                                                     final String patternReplace,
                                                     @Nullable final String replaceInput,
                                                     @Nullable final String patternMatch,
                                                     final boolean firstMatchOnly,
                                                     final PreprocessorRuleMetrics ruleMetrics) {
    super(key, input, patternSearch, patternReplace, replaceInput, patternMatch, firstMatchOnly, ruleMetrics);
  }

  @Override
  public Span apply(@Nonnull Span span) {
    long startNanos = ruleMetrics.ruleStart();
    if (span.getAnnotations() == null) {
      span.setAnnotations(Lists.newArrayList());
    }
    if (span.getAnnotations().stream().noneMatch(a -> a.getKey().equals(key))) {
      internalApply(span);
    }
    ruleMetrics.ruleEnd(startNanos);
    return span;
  }
}
