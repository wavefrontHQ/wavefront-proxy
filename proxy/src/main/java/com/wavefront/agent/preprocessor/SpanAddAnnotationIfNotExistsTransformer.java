package com.wavefront.agent.preprocessor;

import java.util.Map;
import java.util.function.Predicate;

import javax.annotation.Nullable;

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
                                                 @Nullable final Map<String, Object> v2Predicate,
                                                 final PreprocessorRuleMetrics ruleMetrics) {
    super(key, value, v2Predicate, ruleMetrics);
  }

  @Nullable
  @Override
  public Span apply(@Nullable Span span) {
    if (span == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    if (!v2Predicate.test(span)) return span;

    if (span.getAnnotations().stream().noneMatch(a -> a.getKey().equals(key))) {
      span.getAnnotations().add(new Annotation(key, PreprocessorUtil.expandPlaceholders(value, span)));
      ruleMetrics.incrementRuleAppliedCounter();
    }
    ruleMetrics.ruleEnd(startNanos);
    return span;
  }
}
