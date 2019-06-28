package com.wavefront.agent.preprocessor;

import com.google.common.base.Preconditions;

import com.yammer.metrics.core.Counter;

import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * Whitelist regex filter. Reject a point line if it doesn't match the regex
 *
 * Created by Vasily on 9/13/16.
 */
public class PointLineWhitelistRegexFilter implements AnnotatedPredicate<String> {

  private final Pattern compiledPattern;
  private final PreprocessorRuleMetrics ruleMetrics;

  @Deprecated
  public PointLineWhitelistRegexFilter(final String patternMatch,
                                       @Nullable final Counter ruleAppliedCounter) {
    this(patternMatch, new PreprocessorRuleMetrics(ruleAppliedCounter));
  }

  public PointLineWhitelistRegexFilter(final String patternMatch,
                                       final PreprocessorRuleMetrics ruleMetrics) {
    this.compiledPattern = Pattern.compile(Preconditions.checkNotNull(patternMatch, "[match] can't be null"));
    Preconditions.checkArgument(!patternMatch.isEmpty(), "[match] can't be blank");
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
  }

  @Override
  public boolean test(String pointLine, @Nullable String[] messageHolder) {
  long startNanos = ruleMetrics.ruleStart();
    if (!compiledPattern.matcher(pointLine).matches()) {
      ruleMetrics.incrementRuleAppliedCounter();
      ruleMetrics.ruleEnd(startNanos);
      return false;
    }
    ruleMetrics.ruleEnd(startNanos);
    return true;
  }
}
