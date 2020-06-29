package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import wavefront.report.Annotation;
import wavefront.report.Span;

import static com.wavefront.predicates.Util.expandPlaceholders;

/**
 * Create a point tag by extracting a portion of a metric name, source name or another point tag
 *
 * @author vasily@wavefront.com
 */
public class SpanExtractAnnotationTransformer implements Function<Span, Span>{

  protected final String key;
  protected final String input;
  protected final String patternReplace;
  protected final Pattern compiledSearchPattern;
  @Nullable
  protected final Pattern compiledMatchPattern;
  @Nullable
  protected final String patternReplaceInput;
  protected final boolean firstMatchOnly;
  protected final PreprocessorRuleMetrics ruleMetrics;
  protected final Predicate<Span> v2Predicate;

  public SpanExtractAnnotationTransformer(final String key,
                                          final String input,
                                          final String patternSearch,
                                          final String patternReplace,
                                          @Nullable final String replaceInput,
                                          @Nullable final String patternMatch,
                                          final boolean firstMatchOnly,
                                          @Nullable Predicate<Span> v2Predicate,
                                          final PreprocessorRuleMetrics ruleMetrics) {
    this.key = Preconditions.checkNotNull(key, "[key] can't be null");
    this.input = Preconditions.checkNotNull(input, "[input] can't be null");
    this.compiledSearchPattern = Pattern.compile(Preconditions.checkNotNull(patternSearch, "[search] can't be null"));
    this.patternReplace = Preconditions.checkNotNull(patternReplace, "[replace] can't be null");
    Preconditions.checkArgument(!key.isEmpty(), "[key] can't be blank");
    Preconditions.checkArgument(!input.isEmpty(), "[input] can't be blank");
    Preconditions.checkArgument(!patternSearch.isEmpty(), "[search] can't be blank");
    this.compiledMatchPattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    this.patternReplaceInput = replaceInput;
    this.firstMatchOnly = firstMatchOnly;
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
    this.v2Predicate = v2Predicate != null ? v2Predicate : x -> true;
  }

  protected boolean extractAnnotation(@Nonnull Span span, final String extractFrom,
                                      List<Annotation> annotationBuffer) {
    Matcher patternMatcher;
    if (extractFrom == null || (compiledMatchPattern != null && !compiledMatchPattern.matcher(extractFrom).matches())) {
      return false;
    }
    patternMatcher = compiledSearchPattern.matcher(extractFrom);
    if (!patternMatcher.find()) {
      return false;
    }
    String value = patternMatcher.replaceAll(expandPlaceholders(patternReplace, span));
    if (!value.isEmpty()) {
      annotationBuffer.add(new Annotation(key, value));
      ruleMetrics.incrementRuleAppliedCounter();
    }
    return true;
  }

  protected void internalApply(@Nonnull Span span) {
    List<Annotation> buffer = new ArrayList<>();
    switch (input) {
      case "spanName":
        if (extractAnnotation(span, span.getName(), buffer) && patternReplaceInput != null) {
          span.setName(compiledSearchPattern.matcher(span.getName()).
              replaceAll(expandPlaceholders(patternReplaceInput, span)));
        }
        break;
      case "sourceName":
        if (extractAnnotation(span, span.getSource(), buffer) && patternReplaceInput != null) {
          span.setSource(compiledSearchPattern.matcher(span.getSource()).
              replaceAll(expandPlaceholders(patternReplaceInput, span)));
        }
        break;
      default:
        for (Annotation a : span.getAnnotations()) {
          if (a.getKey().equals(input)) {
            if (extractAnnotation(span, a.getValue(), buffer)) {
              if (patternReplaceInput != null) {
                a.setValue(compiledSearchPattern.matcher(a.getValue()).
                    replaceAll(expandPlaceholders(patternReplaceInput, span)));
              }
              if (firstMatchOnly) {
                break;
              }
            }
          }
        }
    }
    span.getAnnotations().addAll(buffer);
  }

  @Nullable
  @Override
  public Span apply(@Nullable Span span) {
    if (span == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (!v2Predicate.test(span)) return span;

      internalApply(span);
      return span;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
