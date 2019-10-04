package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import com.yammer.metrics.core.Counter;

import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import wavefront.report.Span;

/**
 * Rename a given span tag's/annotation's (optional: if its value matches a regex pattern)
 *
 * If the tag matches multiple span annotation keys , all keys will be renamed.
 *
 * @author akodali@vmare.com
 */
public class SpanRenameTagTransformer implements Function<Span, Span> {

  private final String key;
  private final String newKey;
  @Nullable
  private final Pattern compiledPattern;
  private final PreprocessorRuleMetrics ruleMetrics;

  @Deprecated
  public SpanRenameTagTransformer(final String key,
                                  final String newKey,
                                  @Nullable final String patternMatch,
                                  @Nullable final Counter ruleAppliedCounter) {
    this(key, newKey, patternMatch, new PreprocessorRuleMetrics(ruleAppliedCounter));
  }

  public SpanRenameTagTransformer(final String key,
                                  final String newKey,
                                  @Nullable final String patternMatch,
                                  final PreprocessorRuleMetrics ruleMetrics) {
    this.key = Preconditions.checkNotNull(key, "[key] can't be null");
    this.newKey = Preconditions.checkNotNull(newKey, "[newkey] can't be null");
    Preconditions.checkArgument(!key.isEmpty(), "[key] can't be blank");
    Preconditions.checkArgument(!newKey.isEmpty(), "[newkey] can't be blank");
    this.compiledPattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
  }

  @Override
  public Span apply(@Nonnull Span span) {
    long startNanos = ruleMetrics.ruleStart();
    if (span.getAnnotations() == null) {
      ruleMetrics.ruleEnd(startNanos);
      return span;
    }

    span.getAnnotations().stream().
        filter(a -> a.getKey().equals(key) &&
            (compiledPattern == null || compiledPattern.matcher(a.getValue()).matches())).
        forEach(a -> a.setKey(newKey));
    ruleMetrics.incrementRuleAppliedCounter();
    ruleMetrics.ruleEnd(startNanos);
    return span;
  }
}
