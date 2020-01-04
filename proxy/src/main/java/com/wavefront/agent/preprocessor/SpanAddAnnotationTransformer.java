package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;

import wavefront.report.Annotation;
import wavefront.report.Span;

/**
 * Creates a new annotation with a specified key/value pair.
 *
 * @author vasily@wavefront.com
 */
public class SpanAddAnnotationTransformer implements Function<Span, Span> {

  protected final String key;
  protected final String value;
  protected final PreprocessorRuleMetrics ruleMetrics;

  public SpanAddAnnotationTransformer(final String key,
                                      final String value,
                                      final PreprocessorRuleMetrics ruleMetrics) {
    this.key = Preconditions.checkNotNull(key, "[key] can't be null");
    this.value = Preconditions.checkNotNull(value, "[value] can't be null");
    Preconditions.checkArgument(!key.isEmpty(), "[key] can't be blank");
    Preconditions.checkArgument(!value.isEmpty(), "[value] can't be blank");
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
  }

  @Nullable
  @Override
  public Span apply(@Nullable Span span) {
    if (span == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    span.getAnnotations().add(new Annotation(key, PreprocessorUtil.expandPlaceholders(value, span)));
    ruleMetrics.incrementRuleAppliedCounter();
    ruleMetrics.ruleEnd(startNanos);
    return span;
  }
}
