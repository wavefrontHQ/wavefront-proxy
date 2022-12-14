package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import wavefront.report.Annotation;
import wavefront.report.ReportLog;

/**
 * Force lowercase transformer. Converts a specified component of a log (message, source name or a
 * log tag value, depending on "scope" parameter) to lower case to enforce consistency.
 *
 * @author amitw@vmware.com
 */
public class ReportLogForceLowercaseTransformer implements Function<ReportLog, ReportLog> {

  private final String scope;
  @Nullable private final Pattern compiledMatchPattern;
  private final PreprocessorRuleMetrics ruleMetrics;
  private final Predicate<ReportLog> v2Predicate;

  public ReportLogForceLowercaseTransformer(
      final String scope,
      @Nullable final String patternMatch,
      @Nullable Predicate<ReportLog> v2Predicate,
      final PreprocessorRuleMetrics ruleMetrics) {
    this.scope = Preconditions.checkNotNull(scope, "[scope] can't be null");
    Preconditions.checkArgument(!scope.isEmpty(), "[scope] can't be blank");
    this.compiledMatchPattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
    this.v2Predicate = v2Predicate != null ? v2Predicate : x -> true;
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
          reportLog.setMessage(reportLog.getMessage().toLowerCase());
          ruleMetrics.incrementRuleAppliedCounter();
          break;
        case "sourceName": // source name is not case sensitive in Wavefront, but we'll do
          // it anyway
          if (compiledMatchPattern != null
              && !compiledMatchPattern.matcher(reportLog.getHost()).matches()) {
            break;
          }
          reportLog.setHost(reportLog.getHost().toLowerCase());
          ruleMetrics.incrementRuleAppliedCounter();
          break;
        default:
          for (Annotation logTagKV : reportLog.getAnnotations()) {
            if (logTagKV.getKey().equals(scope)
                && (compiledMatchPattern == null
                    || compiledMatchPattern.matcher(logTagKV.getValue()).matches())) {
              logTagKV.setValue(logTagKV.getValue().toLowerCase());
              ruleMetrics.incrementRuleAppliedCounter();
            }
          }
      }
      return reportLog;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
