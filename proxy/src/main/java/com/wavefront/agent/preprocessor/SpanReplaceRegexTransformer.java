package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import wavefront.report.Annotation;
import wavefront.report.Span;

import static com.wavefront.predicates.Util.expandPlaceholders;

/**
 * Replace regex transformer. Performs search and replace on a specified component of a span (span name,
 * source name or an annotation value, depending on "scope" parameter.
 *
 * @author vasily@wavefront.com
 */
public class SpanReplaceRegexTransformer implements Function<Span, Span> {

  private final String patternReplace;
  private final String scope;
  private final Pattern compiledSearchPattern;
  private final Integer maxIterations;
  @Nullable
  private final Pattern compiledMatchPattern;
  private final boolean firstMatchOnly;
  private final PreprocessorRuleMetrics ruleMetrics;
  private final Predicate<Span> v2Predicate;

  public SpanReplaceRegexTransformer(final String scope,
                                     final String patternSearch,
                                     final String patternReplace,
                                     @Nullable final String patternMatch,
                                     @Nullable final Integer maxIterations,
                                     final boolean firstMatchOnly,
                                     @Nullable final Predicate<Span> v2Predicate,
                                     final PreprocessorRuleMetrics ruleMetrics) {
    this.compiledSearchPattern = Pattern.compile(Preconditions.checkNotNull(patternSearch, "[search] can't be null"));
    Preconditions.checkArgument(!patternSearch.isEmpty(), "[search] can't be blank");
    this.scope = Preconditions.checkNotNull(scope, "[scope] can't be null");
    Preconditions.checkArgument(!scope.isEmpty(), "[scope] can't be blank");
    this.patternReplace = Preconditions.checkNotNull(patternReplace, "[replace] can't be null");
    this.compiledMatchPattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    this.maxIterations = maxIterations != null ? maxIterations : 1;
    Preconditions.checkArgument(this.maxIterations > 0, "[iterations] must be > 0");
    this.firstMatchOnly = firstMatchOnly;
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
    this.v2Predicate = v2Predicate != null ? v2Predicate : x -> true;
  }

  private String replaceString(@Nonnull Span span, String content) {
    Matcher patternMatcher;
    patternMatcher = compiledSearchPattern.matcher(content);
    if (!patternMatcher.find()) {
      return content;
    }
    ruleMetrics.incrementRuleAppliedCounter();

    String replacement = expandPlaceholders(patternReplace, span);

    int currentIteration = 0;
    while (currentIteration < maxIterations) {
      content = patternMatcher.replaceAll(replacement);
      patternMatcher = compiledSearchPattern.matcher(content);
      if (!patternMatcher.find()) {
        break;
      }
      currentIteration++;
    }
    return content;
  }

  @Nullable
  @Override
  public Span apply(@Nullable Span span) {
    if (span == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (!v2Predicate.test(span)) return span;

      switch (scope) {
        case "spanName":
          if (compiledMatchPattern != null && !compiledMatchPattern.matcher(span.getName()).matches()) {
            break;
          }
          span.setName(replaceString(span, span.getName()));
          break;
        case "sourceName":
          if (compiledMatchPattern != null && !compiledMatchPattern.matcher(span.getSource()).matches()) {
            break;
          }
          span.setSource(replaceString(span, span.getSource()));
          break;
        default:
          for (Annotation x : span.getAnnotations()) {
            if (x.getKey().equals(scope) && (compiledMatchPattern == null ||
                compiledMatchPattern.matcher(x.getValue()).matches())) {
              String newValue = replaceString(span, x.getValue());
              if (!newValue.equals(x.getValue())) {
                x.setValue(newValue);
                if (firstMatchOnly) {
                  break;
                }
              }
            }
          }
      }
      return span;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
