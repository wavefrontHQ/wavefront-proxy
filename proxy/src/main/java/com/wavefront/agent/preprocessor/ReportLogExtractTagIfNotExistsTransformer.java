package com.wavefront.agent.preprocessor;

import java.util.function.Predicate;
import javax.annotation.Nullable;
import wavefront.report.ReportLog;

/**
 * Create a log tag by extracting a portion of a message, source name or another log tag. If such
 * log tag already exists, the value won't be overwritten.
 */
public class ReportLogExtractTagIfNotExistsTransformer extends ReportLogExtractTagTransformer {

  public ReportLogExtractTagIfNotExistsTransformer(
      final String tag,
      final String input,
      final String patternSearch,
      final String patternReplace,
      @Nullable final String replaceInput,
      @Nullable final String patternMatch,
      @Nullable final Predicate<ReportLog> v2Predicate,
      final PreprocessorRuleMetrics ruleMetrics) {
    super(
        tag,
        input,
        patternSearch,
        patternReplace,
        replaceInput,
        patternMatch,
        v2Predicate,
        ruleMetrics);
  }

  @Nullable
  @Override
  public ReportLog apply(@Nullable ReportLog reportLog) {
    if (reportLog == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (!v2Predicate.test(reportLog)) return reportLog;

      if (reportLog.getAnnotations().stream().noneMatch(a -> a.getKey().equals(tag))) {
        internalApply(reportLog);
      }
      return reportLog;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
