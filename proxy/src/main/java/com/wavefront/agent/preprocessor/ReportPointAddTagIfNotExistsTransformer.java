package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import com.yammer.metrics.core.Counter;

import javax.annotation.Nullable;
import javax.annotation.Nonnull;

import wavefront.report.ReportPoint;

/**
 * Creates a new point tag with a specified value. If such point tag already exists, the value won't be overwritten.
 *
 * Created by Vasily on 9/13/16.
 */
public class ReportPointAddTagIfNotExistsTransformer extends ReportPointAddTagTransformer {

  @Deprecated
  public ReportPointAddTagIfNotExistsTransformer(final String tag,
                                                 final String value,
                                                 @Nullable final Counter ruleAppliedCounter) {
    this(tag, value, new PreprocessorRuleMetrics(ruleAppliedCounter));
  }

  public ReportPointAddTagIfNotExistsTransformer(final String tag,
                                                 final String value,
                                                 final PreprocessorRuleMetrics ruleMetrics) {
    super(tag, value, ruleMetrics);
  }

  @Override
  public ReportPoint apply(@Nonnull ReportPoint reportPoint) {
    long startNanos = ruleMetrics.ruleStart();
    if (reportPoint.getAnnotations() == null) {
      reportPoint.setAnnotations(Maps.<String, String>newHashMap());
    }
    if (reportPoint.getAnnotations().get(tag) == null) {
      reportPoint.getAnnotations().put(tag, PreprocessorUtil.expandPlaceholders(value, reportPoint));
      ruleMetrics.incrementRuleAppliedCounter();
    }
    ruleMetrics.ruleEnd(startNanos);
    return reportPoint;
  }
}
