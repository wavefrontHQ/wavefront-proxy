package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import com.yammer.metrics.core.Counter;

import javax.annotation.Nullable;

import sunnylabs.report.ReportPoint;

/**
 * Creates a new point tag with a specified value, or overwrite an existing one.
 *
 * Created by Vasily on 9/13/16.
 */
public class ReportPointAddTagTransformer implements Function<ReportPoint, ReportPoint> {

  private final String tag;
  private final String value;
  private final Counter ruleAppliedCounter;

  public ReportPointAddTagTransformer(final String tag,
                                      final String value,
                                      @Nullable final Counter ruleAppliedCounter) {
    Preconditions.checkNotNull(tag, "[tag] can't be null");
    Preconditions.checkNotNull(value, "[value] can't be null");
    Preconditions.checkArgument(!value.isEmpty(), "[value] can't be blank");
    this.value = value;
    this.tag = tag;
    this.ruleAppliedCounter = ruleAppliedCounter;
  }

  @Override
  public ReportPoint apply(ReportPoint reportPoint) {
    if (reportPoint.getAnnotations() == null) {
      reportPoint.setAnnotations(Maps.<String, String>newHashMap());
    }
    reportPoint.getAnnotations().put(tag, value);
    if (ruleAppliedCounter != null) {
      ruleAppliedCounter.inc();
    }
    return reportPoint;
  }
}
