package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.annotation.Nonnull;

import wavefront.report.ReportPoint;

import static com.wavefront.predicates.Util.expandPlaceholders;

/**
 * Replace regex transformer. Performs search and replace on a specified component of a point (metric name,
 * source name or a point tag value, depending on "scope" parameter.
 *
 * Created by Vasily on 9/13/16.
 */
public class ReportPointReplaceRegexTransformer implements Function<ReportPoint, ReportPoint> {

  private final String patternReplace;
  private final String scope;
  private final Pattern compiledSearchPattern;
  private final Integer maxIterations;
  @Nullable
  private final Pattern compiledMatchPattern;
  private final PreprocessorRuleMetrics ruleMetrics;
  private final Predicate<ReportPoint> v2Predicate;

  public ReportPointReplaceRegexTransformer(final String scope,
                                            final String patternSearch,
                                            final String patternReplace,
                                            @Nullable final String patternMatch,
                                            @Nullable final Integer maxIterations,
                                            @Nullable final Predicate<ReportPoint> v2Predicate,
                                            final PreprocessorRuleMetrics ruleMetrics) {
    this.compiledSearchPattern = Pattern.compile(Preconditions.checkNotNull(patternSearch, "[search] can't be null"));
    Preconditions.checkArgument(!patternSearch.isEmpty(), "[search] can't be blank");
    this.scope = Preconditions.checkNotNull(scope, "[scope] can't be null");
    Preconditions.checkArgument(!scope.isEmpty(), "[scope] can't be blank");
    this.patternReplace = Preconditions.checkNotNull(patternReplace, "[replace] can't be null");
    this.compiledMatchPattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    this.maxIterations = maxIterations != null ? maxIterations : 1;
    Preconditions.checkArgument(this.maxIterations > 0, "[iterations] must be > 0");
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
    this.v2Predicate = v2Predicate != null ? v2Predicate : x -> true;

  }

  private String replaceString(@Nonnull ReportPoint reportPoint, String content) {
    Matcher patternMatcher;
    patternMatcher = compiledSearchPattern.matcher(content);
    if (!patternMatcher.find()) {
      return content;
    }
    ruleMetrics.incrementRuleAppliedCounter();

    String replacement = expandPlaceholders(patternReplace, reportPoint);

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
  public ReportPoint apply(@Nullable ReportPoint reportPoint) {
    if (reportPoint == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (!v2Predicate.test(reportPoint)) return reportPoint;

      switch (scope) {
        case "metricName":
          if (compiledMatchPattern != null && !compiledMatchPattern.matcher(reportPoint.getMetric()).matches()) {
            break;
          }
          reportPoint.setMetric(replaceString(reportPoint, reportPoint.getMetric()));
          break;
        case "sourceName":
          if (compiledMatchPattern != null && !compiledMatchPattern.matcher(reportPoint.getHost()).matches()) {
            break;
          }
          reportPoint.setHost(replaceString(reportPoint, reportPoint.getHost()));
          break;
        default:
          if (reportPoint.getAnnotations() != null) {
            String tagValue = reportPoint.getAnnotations().get(scope);
            if (tagValue != null) {
              if (compiledMatchPattern != null && !compiledMatchPattern.matcher(tagValue).matches()) {
                break;
              }
              reportPoint.getAnnotations().put(scope, replaceString(reportPoint, tagValue));
            }
          }
      }
      return reportPoint;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
