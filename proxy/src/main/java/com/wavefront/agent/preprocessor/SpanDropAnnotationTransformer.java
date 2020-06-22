package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import wavefront.report.Annotation;
import wavefront.report.Span;

/**
 * Removes a span annotation with a specific key if its value matches an optional regex pattern (always remove if null)
 *
 * @author vasily@wavefront.com
 */
public class SpanDropAnnotationTransformer implements Function<Span, Span> {

  private final Pattern compiledKeyPattern;
  @Nullable
  private final Pattern compiledValuePattern;
  private final boolean firstMatchOnly;
  private final PreprocessorRuleMetrics ruleMetrics;
  private final Predicate<Span> v2Predicate;


  public SpanDropAnnotationTransformer(final String key,
                                       @Nullable final String patternMatch,
                                       final boolean firstMatchOnly,
                                       @Nullable final Predicate<Span> v2Predicate,
                                       final PreprocessorRuleMetrics ruleMetrics) {
    this.compiledKeyPattern = Pattern.compile(Preconditions.checkNotNull(key, "[key] can't be null"));
    Preconditions.checkArgument(!key.isEmpty(), "[key] can't be blank");
    this.compiledValuePattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.firstMatchOnly = firstMatchOnly;
    this.ruleMetrics = ruleMetrics;
    this.v2Predicate = v2Predicate != null ? v2Predicate : x -> true;
  }

  @Nullable
  @Override
  public Span apply(@Nullable Span span) {
    if (span == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (!v2Predicate.test(span)) return span;

      List<Annotation> annotations = new ArrayList<>(span.getAnnotations());
      Iterator<Annotation> iterator = annotations.iterator();
      boolean changed = false;
      while (iterator.hasNext()) {
        Annotation entry = iterator.next();
        if (compiledKeyPattern.matcher(entry.getKey()).matches() && (compiledValuePattern == null ||
            compiledValuePattern.matcher(entry.getValue()).matches())) {
          changed = true;
          iterator.remove();
          ruleMetrics.incrementRuleAppliedCounter();
          if (firstMatchOnly) {
            break;
          }
        }
      }
      if (changed) {
        span.setAnnotations(annotations);
      }
      return span;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
