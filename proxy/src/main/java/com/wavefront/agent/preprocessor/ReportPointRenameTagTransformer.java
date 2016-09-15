package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import com.yammer.metrics.core.Counter;

import java.util.regex.Pattern;

import javax.annotation.Nullable;

import sunnylabs.report.ReportPoint;

/**
 * Rename a point tag (optional: if its value matches a regex pattern)
 *
 * Created by Vasily on 9/13/16.
 */
public class ReportPointRenameTagTransformer implements Function<ReportPoint, ReportPoint> {

  private final String tag;
  private final String newTag;
  private final Pattern compiledPattern;
  private final Counter ruleAppliedCounter;

  public ReportPointRenameTagTransformer(final String tag,
                                         final String newTag,
                                         @Nullable final String patternMatch,
                                         @Nullable final Counter ruleAppliedCounter) {
    Preconditions.checkNotNull(tag, "[tag] can't be null");
    Preconditions.checkNotNull(newTag, "[newtag] can't be null");
    Preconditions.checkArgument(!newTag.isEmpty(), "[newtag] can't be blank");
    this.newTag = newTag;
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
    reportPoint.getAnnotations().put(newTag, tagValue);
    if (ruleAppliedCounter != null) {
      ruleAppliedCounter.inc();
    }
    return reportPoint;
  }
}
