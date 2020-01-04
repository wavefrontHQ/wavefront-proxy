package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import com.yammer.metrics.core.Counter;

import java.util.regex.Pattern;

import javax.annotation.Nullable;

import wavefront.report.ReportPoint;

/**
 * Rename a point tag (optional: if its value matches a regex pattern)
 *
 * Created by Vasily on 9/13/16.
 */
public class ReportPointRenameTagTransformer implements Function<ReportPoint, ReportPoint> {

  private final String tag;
  private final String newTag;
  @Nullable
  private final Pattern compiledPattern;
  private final PreprocessorRuleMetrics ruleMetrics;

  public ReportPointRenameTagTransformer(final String tag,
                                         final String newTag,
                                         @Nullable final String patternMatch,
                                         final PreprocessorRuleMetrics ruleMetrics) {
    this.tag = Preconditions.checkNotNull(tag, "[tag] can't be null");
    this.newTag = Preconditions.checkNotNull(newTag, "[newtag] can't be null");
    Preconditions.checkArgument(!tag.isEmpty(), "[tag] can't be blank");
    Preconditions.checkArgument(!newTag.isEmpty(), "[newtag] can't be blank");
    this.compiledPattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
  }

  @Nullable
  @Override
  public ReportPoint apply(@Nullable ReportPoint reportPoint) {
    if (reportPoint == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    String tagValue = reportPoint.getAnnotations().get(tag);
    if (tagValue == null || (compiledPattern != null && !compiledPattern.matcher(tagValue).matches())) {
      ruleMetrics.ruleEnd(startNanos);
      return reportPoint;
    }
    reportPoint.getAnnotations().remove(tag);
    reportPoint.getAnnotations().put(newTag, tagValue);
    ruleMetrics.incrementRuleAppliedCounter();
    ruleMetrics.ruleEnd(startNanos);
    return reportPoint;
  }
}
