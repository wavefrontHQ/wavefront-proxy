package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import com.yammer.metrics.core.Counter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * Replace regex transformer. Performs search and replace on an entire point line string
 *
 * Created by Vasily on 9/13/16.
 */
public class PointLineReplaceRegexTransformer implements Function<String, String> {

  private final String patternReplace;
  private final Pattern compiledPattern;
  @Nullable
  private final Counter ruleAppliedCounter;

  public PointLineReplaceRegexTransformer(final String patternMatch,
                                          final String patternReplace,
                                          @Nullable final Counter ruleAppliedCounter) {
    Preconditions.checkNotNull(patternMatch, "[match] can't be null");
    Preconditions.checkNotNull(patternReplace, "[replace] can't be null");
    Preconditions.checkArgument(!patternMatch.isEmpty(), "[match] can't be blank");
    this.patternReplace = patternReplace;
    this.compiledPattern = Pattern.compile(patternMatch);
    this.ruleAppliedCounter = ruleAppliedCounter;
  }

  @Override
  public String apply(String pointLine)
  {
    Matcher patternMatcher = compiledPattern.matcher(pointLine);
    if (patternMatcher.find()) {
      if (ruleAppliedCounter != null) {
        ruleAppliedCounter.inc();
      }
      return patternMatcher.replaceAll(patternReplace);
    }
    return pointLine;
  }
}
