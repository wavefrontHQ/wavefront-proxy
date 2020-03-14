package com.wavefront.agent.preprocessor;

import java.util.Map;

import javax.annotation.Nullable;

import wavefront.report.ReportPoint;

/**
 * Creates a new point tag with a specified value. If such point tag already exists, the value won't be overwritten.
 *
 * Created by Vasily on 9/13/16.
 */
public class ReportPointAddTagIfNotExistsTransformer extends ReportPointAddTagTransformer {

  @Nullable
  private final Map<String, Object> v2Predicate;

  public ReportPointAddTagIfNotExistsTransformer(final String tag,
                                                 final String value,
                                                 @Nullable final Map<String, Object> v2Predicate,
                                                 final PreprocessorRuleMetrics ruleMetrics) {
    super(tag, value, v2Predicate, ruleMetrics);
    this.v2Predicate = v2Predicate;
  }

  @Nullable
  @Override
  public ReportPoint apply(@Nullable ReportPoint reportPoint) {
    if (reportPoint == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    // Test for preprocessor v2 predicate.
    if (!PreprocessorUtil.isRuleApplicable(v2Predicate, reportPoint)) return reportPoint;

    if (reportPoint.getAnnotations().get(tag) == null) {
      reportPoint.getAnnotations().put(tag, PreprocessorUtil.expandPlaceholders(value, reportPoint));
      ruleMetrics.incrementRuleAppliedCounter();
    }
    ruleMetrics.ruleEnd(startNanos);
    return reportPoint;
  }
}
