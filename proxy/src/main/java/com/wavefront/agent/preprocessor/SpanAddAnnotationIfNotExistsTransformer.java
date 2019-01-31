package com.wavefront.agent.preprocessor;

import com.google.common.collect.Lists;

import javax.annotation.Nonnull;

import wavefront.report.Annotation;
import wavefront.report.Span;

/**
 * Creates a new annotation with a specified key/value pair.
 * If such point tag already exists, the value won't be overwritten.
 *
 * @author vasily@wavefront.com
 */
public class SpanAddAnnotationIfNotExistsTransformer extends SpanAddAnnotationTransformer {

  public SpanAddAnnotationIfNotExistsTransformer(final String key,
                                                 final String value,
                                                 final PreprocessorRuleMetrics ruleMetrics) {
    super(key, value, ruleMetrics);
  }

  @Override
  public Span apply(@Nonnull Span span) {
    long startNanos = ruleMetrics.ruleStart();
    if (span.getAnnotations() == null) {
      span.setAnnotations(Lists.newArrayList());
    }
    if (span.getAnnotations().stream().noneMatch(a -> a.getKey().equals(key))) {
      span.getAnnotations().add(new Annotation(key, PreprocessorUtil.expandPlaceholders(value, span)));
      ruleMetrics.incrementRuleAppliedCounter();
    }
    ruleMetrics.ruleEnd(startNanos);
    return span;
  }
}
