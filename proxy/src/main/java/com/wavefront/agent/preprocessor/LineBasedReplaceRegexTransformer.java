package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * Replace regex transformer. Performs search and replace on an entire point line string
 *
 * Created by Vasily on 9/13/16.
 */
public class LineBasedReplaceRegexTransformer implements Function<String, String> {

  private final String patternReplace;
  private final Pattern compiledSearchPattern;
  private final Integer maxIterations;
  @Nullable
  private final Pattern compiledMatchPattern;
  private final PreprocessorRuleMetrics ruleMetrics;

  public LineBasedReplaceRegexTransformer(final String patternSearch,
                                          final String patternReplace,
                                          @Nullable final String patternMatch,
                                          @Nullable final Integer maxIterations,
                                          final PreprocessorRuleMetrics ruleMetrics) {
    this.compiledSearchPattern = Pattern.compile(Preconditions.checkNotNull(patternSearch, "[search] can't be null"));
    Preconditions.checkArgument(!patternSearch.isEmpty(), "[search] can't be blank");
    this.compiledMatchPattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    this.patternReplace = Preconditions.checkNotNull(patternReplace, "[replace] can't be null");
    this.maxIterations = maxIterations != null ? maxIterations : 1;
    Preconditions.checkArgument(this.maxIterations > 0, "[iterations] must be > 0");
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
  }

  @Override
  public String apply(String pointLine) {
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (compiledMatchPattern != null && !compiledMatchPattern.matcher(pointLine).matches()) {
        return pointLine;
      }
      Matcher patternMatcher = compiledSearchPattern.matcher(pointLine);

      if (!patternMatcher.find()) {
        return pointLine;
      }
      ruleMetrics.incrementRuleAppliedCounter(); // count the rule only once regardless of the number of iterations

      int currentIteration = 0;
      while (true) {
        pointLine = patternMatcher.replaceAll(patternReplace);
        currentIteration++;
        if (currentIteration >= maxIterations) {
          break;
        }
        patternMatcher = compiledSearchPattern.matcher(pointLine);
        if (!patternMatcher.find()) {
          break;
        }
      }
      return pointLine;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
