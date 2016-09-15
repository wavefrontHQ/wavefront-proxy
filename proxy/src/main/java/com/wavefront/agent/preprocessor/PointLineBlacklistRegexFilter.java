package com.wavefront.agent.preprocessor;

import com.google.common.base.Preconditions;

import com.yammer.metrics.core.Counter;

import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * Blacklist regex filter. Reject a point line if it matches the regex
 *
 * Created by Vasily on 9/13/16.
 */
public class PointLineBlacklistRegexFilter extends AnnotatedPredicate<String> {

  private final Pattern compiledPattern;
  @Nullable
  private final Counter ruleAppliedCounter;

  public PointLineBlacklistRegexFilter(final String patternMatch,
                                       @Nullable final Counter ruleAppliedCounter) {
    Preconditions.checkNotNull(patternMatch, "[match] can't be null");
    this.compiledPattern = Pattern.compile(patternMatch);
    this.ruleAppliedCounter = ruleAppliedCounter;
  }

  @Override
  public boolean apply(String pointLine) {
    if (compiledPattern.matcher(pointLine).matches()) {
      if (ruleAppliedCounter != null) {
        ruleAppliedCounter.inc();
      }
      return false;
    }
  return true;
  }
}
