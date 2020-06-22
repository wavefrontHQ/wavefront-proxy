package com.wavefront.agent.preprocessor;

import java.util.function.Predicate;

import javax.annotation.Nullable;

import wavefront.report.ReportPoint;

/**
 * Create a point tag by extracting a portion of a metric name, source name or another point tag.
 * If such point tag already exists, the value won't be overwritten.
 *
 * @author vasily@wavefront.com
 * Created 5/18/18
 */
public class ReportPointExtractTagIfNotExistsTransformer extends ReportPointExtractTagTransformer {

  public ReportPointExtractTagIfNotExistsTransformer(final String tag,
                                                     final String source,
                                                     final String patternSearch,
                                                     final String patternReplace,
                                                     @Nullable final String replaceSource,
                                                     @Nullable final String patternMatch,
                                                     @Nullable final Predicate<ReportPoint> v2Predicate,
                                                     final PreprocessorRuleMetrics ruleMetrics) {
    super(tag, source, patternSearch, patternReplace, replaceSource, patternMatch, v2Predicate, ruleMetrics);
  }

  @Nullable
  @Override
  public ReportPoint apply(@Nullable ReportPoint reportPoint) {
    if (reportPoint == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (!v2Predicate.test(reportPoint)) return reportPoint;

      if (reportPoint.getAnnotations().get(tag) == null) {
        internalApply(reportPoint);
      }
      return reportPoint;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
