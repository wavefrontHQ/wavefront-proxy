package com.wavefront.agent.preprocessor;

import static com.wavefront.predicates.Util.expandPlaceholders;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import wavefront.report.Annotation;
import wavefront.report.ReportLog;

/**
 * Replace regex transformer. Performs search and replace on a specified component of a log
 * (message, source name or a log tag value, depending on "scope" parameter.
 *
 * @author amitw@vmware.com
 */
public class ReportLogReplaceRegexTransformer implements Function<ReportLog, ReportLog> {

  private final String patternReplace;
  private final String scope;
  private final Pattern compiledSearchPattern;
  private final Integer maxIterations;
  @Nullable private final Pattern compiledMatchPattern;
  private final PreprocessorRuleMetrics ruleMetrics;
  private final Predicate<ReportLog> v2Predicate;

  public ReportLogReplaceRegexTransformer(
      final String scope,
      final String patternSearch,
      final String patternReplace,
      @Nullable final String patternMatch,
      @Nullable final Integer maxIterations,
      @Nullable final Predicate<ReportLog> v2Predicate,
      final PreprocessorRuleMetrics ruleMetrics) {
    this.compiledSearchPattern =
        Pattern.compile(Preconditions.checkNotNull(patternSearch, "[search] can't be null"));
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

  private String replaceString(@Nonnull ReportLog reportLog, String content) {
    Matcher patternMatcher;
    patternMatcher = compiledSearchPattern.matcher(content);
    if (!patternMatcher.find()) {
      return content;
    }
    ruleMetrics.incrementRuleAppliedCounter();

    String replacement = expandPlaceholders(patternReplace, reportLog);

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
  public ReportLog apply(@Nullable ReportLog reportLog) {
    if (reportLog == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (!v2Predicate.test(reportLog)) return reportLog;

      switch (scope) {
        case "message":
          if (compiledMatchPattern != null
              && !compiledMatchPattern.matcher(reportLog.getMessage()).matches()) {
            break;
          }
          reportLog.setMessage(replaceString(reportLog, reportLog.getMessage()));
          break;
        case "sourceName":
          if (compiledMatchPattern != null
              && !compiledMatchPattern.matcher(reportLog.getHost()).matches()) {
            break;
          }
          reportLog.setHost(replaceString(reportLog, reportLog.getHost()));
          break;
        default:
          for (Annotation tagKV : reportLog.getAnnotations()) {
            if (tagKV.getKey().equals(scope)
                && (compiledMatchPattern == null
                    || compiledMatchPattern.matcher(tagKV.getValue()).matches())) {
              String newValue = replaceString(reportLog, tagKV.getValue());
              if (!newValue.equals(tagKV.getValue())) {
                tagKV.setValue(newValue);
                break;
              }
            }
          }
      }
      return reportLog;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
