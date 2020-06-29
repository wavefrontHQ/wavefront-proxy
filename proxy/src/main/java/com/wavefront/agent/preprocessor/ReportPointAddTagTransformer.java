package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import java.util.function.Predicate;

import javax.annotation.Nullable;

import wavefront.report.ReportPoint;

import static com.wavefront.predicates.Util.expandPlaceholders;

/**
 * Creates a new point tag with a specified value, or overwrite an existing one.
 *
 * Created by Vasily on 9/13/16.
 */
public class ReportPointAddTagTransformer implements Function<ReportPoint, ReportPoint> {

  protected final String tag;
  protected final String value;
  protected final PreprocessorRuleMetrics ruleMetrics;
  protected final Predicate<ReportPoint> v2Predicate;

  public ReportPointAddTagTransformer(final String tag,
                                      final String value,
                                      @Nullable final Predicate<ReportPoint> v2Predicate,
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
  public ReportPoint apply(@Nullable ReportPoint reportPoint) {
    if (reportPoint == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (!v2Predicate.test(reportPoint)) return reportPoint;

      reportPoint.getAnnotations().put(tag, expandPlaceholders(value, reportPoint));
      ruleMetrics.incrementRuleAppliedCounter();
      return reportPoint;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
