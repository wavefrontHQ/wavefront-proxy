package com.wavefront.agent.preprocessor;

import static com.wavefront.predicates.Util.expandPlaceholders;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import wavefront.report.Annotation;
import wavefront.report.ReportLog;

/**
 * Creates a new log tag with a specified value, or overwrite an existing one.
 *
 * @author amitw@wavefront.com
 */
public class ReportLogAddTagTransformer implements Function<ReportLog, ReportLog> {

  protected final String tag;
  protected final String value;
  protected final PreprocessorRuleMetrics ruleMetrics;
  protected final Predicate<ReportLog> v2Predicate;

  public ReportLogAddTagTransformer(
      final String tag,
      final String value,
      @Nullable final Predicate<ReportLog> v2Predicate,
      final PreprocessorRuleMetrics ruleMetrics) {
    this.tag = Preconditions.checkNotNull(tag, "[tag] can't be null");
    this.value = Preconditions.checkNotNull(value, "[value] can't be null");
    Preconditions.checkArgument(!tag.isEmpty(), "[tag] can't be blank");
    Preconditions.checkArgument(!value.isEmpty(), "[value] can't be blank");
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

      reportLog.getAnnotations().add(new Annotation(tag, expandPlaceholders(value, reportLog)));
      ruleMetrics.incrementRuleAppliedCounter();
      return reportLog;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
