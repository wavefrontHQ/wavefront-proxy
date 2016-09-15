package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import com.yammer.metrics.core.Counter;

import java.util.regex.Pattern;

import javax.annotation.Nullable;

import sunnylabs.report.ReportPoint;

/**
 * Removes a point tag if its value matches an optional regex pattern (always remove if null)
 *
 * Created by Vasily on 9/13/16.
 */
public class ReportPointDropTagTransformer implements Function<ReportPoint, ReportPoint> {

  private final String tag;
  @Nullable
  private final Pattern compiledPattern;
  @Nullable
  private final Counter ruleAppliedCounter;

  public ReportPointDropTagTransformer(final String tag,
                                       @Nullable final String patternMatch,
                                       @Nullable final Counter ruleAppliedCounter) {
    Preconditions.checkNotNull(tag, "[tag] can't be null");
    this.tag = tag;
    this.compiledPattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    this.ruleAppliedCounter = ruleAppliedCounter;
  }

  @Override
  public ReportPoint apply(ReportPoint reportPoint) {
    if (reportPoint.getAnnotations() == null) {
      return reportPoint;
    }
    String tagValue = reportPoint.getAnnotations().get(tag);
    if (tagValue == null || (compiledPattern != null && !compiledPattern.matcher(tagValue).matches())) {
      return reportPoint;
    }
    reportPoint.getAnnotations().remove(tag);
    if (ruleAppliedCounter != null) {
      ruleAppliedCounter.inc();
    }
    return reportPoint;
  }
}
