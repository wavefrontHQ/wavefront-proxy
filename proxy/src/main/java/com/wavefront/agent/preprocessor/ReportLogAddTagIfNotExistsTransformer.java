package com.wavefront.agent.preprocessor;

import static com.wavefront.predicates.Util.expandPlaceholders;

import java.util.function.Predicate;
import javax.annotation.Nullable;
import wavefront.report.Annotation;
import wavefront.report.ReportLog;

/**
 * Creates a new log tag with a specified value. If such log tag already exists, the value won't be
 * overwritten.
 *
 * @author amitw@vmware.com
 */
public class ReportLogAddTagIfNotExistsTransformer extends ReportLogAddTagTransformer {

  public ReportLogAddTagIfNotExistsTransformer(
      final String tag,
      final String value,
      @Nullable final Predicate<ReportLog> v2Predicate,
      final PreprocessorRuleMetrics ruleMetrics) {
    super(tag, value, v2Predicate, ruleMetrics);
  }

  @Nullable
  @Override
  public ReportLog apply(@Nullable ReportLog reportLog) {
    if (reportLog == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (!v2Predicate.test(reportLog)) return reportLog;

      if (reportLog.getAnnotations().stream().noneMatch(a -> a.getKey().equals(tag))) {
        reportLog.getAnnotations().add(new Annotation(tag, expandPlaceholders(value, reportLog)));
        ruleMetrics.incrementRuleAppliedCounter();
      }
      return reportLog;

    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
