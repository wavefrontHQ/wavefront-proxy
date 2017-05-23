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
  private final Pattern compiledSearchPattern;
  @Nullable
  private final Pattern compiledMatchPattern;
  @Nullable
  private final Counter ruleAppliedCounter;

  public PointLineReplaceRegexTransformer(final String patternSearch,
                                          final String patternReplace,
                                          @Nullable final String patternMatch,
                                          @Nullable final Counter ruleAppliedCounter) {
    this.compiledSearchPattern = Pattern.compile(Preconditions.checkNotNull(patternSearch, "[search] can't be null"));
    Preconditions.checkArgument(!patternSearch.isEmpty(), "[search] can't be blank");
    this.compiledMatchPattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    this.patternReplace = Preconditions.checkNotNull(patternReplace, "[replace] can't be null");
    this.ruleAppliedCounter = ruleAppliedCounter;
  }

  @Override
  public String apply(String pointLine)
  {
    if (compiledMatchPattern != null && !compiledMatchPattern.matcher(pointLine).matches()) {
      return pointLine;
    }
    Matcher patternMatcher = compiledSearchPattern.matcher(pointLine);
    if (patternMatcher.find()) {
      if (ruleAppliedCounter != null) {
        ruleAppliedCounter.inc();
      }
      return patternMatcher.replaceAll(patternReplace);
    }
    return pointLine;
  }
}
