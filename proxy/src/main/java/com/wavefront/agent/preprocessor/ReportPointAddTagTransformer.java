package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import com.yammer.metrics.core.Counter;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import wavefront.report.ReportPoint;

/**
 * Creates a new point tag with a specified value, or overwrite an existing one.
 *
 * Created by Vasily on 9/13/16.
 */
public class ReportPointAddTagTransformer implements Function<ReportPoint, ReportPoint> {

  protected final String tag;
  protected final String value;
  protected final PreprocessorRuleMetrics ruleMetrics;

  @Deprecated
  public ReportPointAddTagTransformer(final String tag,
                                      final String value,
                                      @Nullable final Counter ruleAppliedCounter) {
    this(tag, value, new PreprocessorRuleMetrics(ruleAppliedCounter));
  }

  public ReportPointAddTagTransformer(final String tag,
                                      final String value,
                                      final PreprocessorRuleMetrics ruleMetrics) {
    this.tag = Preconditions.checkNotNull(tag, "[tag] can't be null");
    this.value = Preconditions.checkNotNull(value, "[value] can't be null");
    Preconditions.checkArgument(!tag.isEmpty(), "[tag] can't be blank");
    Preconditions.checkArgument(!value.isEmpty(), "[value] can't be blank");
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
  }

  @Override
  public ReportPoint apply(@NotNull ReportPoint reportPoint) {
    long startNanos = ruleMetrics.ruleStart();
    if (reportPoint.getAnnotations() == null) {
      reportPoint.setAnnotations(Maps.<String, String>newHashMap());
    }
    reportPoint.getAnnotations().put(tag, PreprocessorUtil.expandPlaceholders(value, reportPoint));
    ruleMetrics.incrementRuleAppliedCounter();
    ruleMetrics.ruleEnd(startNanos);
    return reportPoint;
  }
}
