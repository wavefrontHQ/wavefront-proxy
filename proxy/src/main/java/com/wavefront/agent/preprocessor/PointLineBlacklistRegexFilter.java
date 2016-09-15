package com.wavefront.agent.preprocessor;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

import com.yammer.metrics.core.Counter;

import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * Created by Vasily on 9/13/16.
 */
public class PointLineBlacklistRegexFilter implements Predicate<String> {
  private final Pattern compiledPattern;
  private final Counter ruleAppliedCounter;

  public PointLineBlacklistRegexFilter(final String patternMatch,
                                       @Nullable final Counter ruleAppliedCounter) {
    Preconditions.checkNotNull(patternMatch);
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
