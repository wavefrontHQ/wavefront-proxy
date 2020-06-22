package com.wavefront.agent.preprocessor;

import com.google.common.base.Preconditions;

import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * Blocking regex-based filter. Reject a point line if it matches the regex.
 *
 * Created by Vasily on 9/13/16.
 */
public class LineBasedBlockFilter implements AnnotatedPredicate<String> {

  private final Pattern compiledPattern;
  private final PreprocessorRuleMetrics ruleMetrics;

  public LineBasedBlockFilter(final String patternMatch,
                              final PreprocessorRuleMetrics ruleMetrics) {
    this.compiledPattern = Pattern.compile(Preconditions.checkNotNull(patternMatch, "[match] can't be null"));
    Preconditions.checkArgument(!patternMatch.isEmpty(), "[match] can't be blank");
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
  }

  @Override
  public boolean test(String pointLine, @Nullable String[] messageHolder) {
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (compiledPattern.matcher(pointLine).matches()) {
        ruleMetrics.incrementRuleAppliedCounter();
        return false;
      }
      return true;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
