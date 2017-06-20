package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import com.yammer.metrics.core.Counter;

import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import wavefront.report.ReportPoint;

/**
 * Removes a point tag if its value matches an optional regex pattern (always remove if null)
 *
 * Created by Vasily on 9/13/16.
 */
public class ReportPointDropTagTransformer implements Function<ReportPoint, ReportPoint> {

  @Nullable
  private final Pattern compiledTagPattern;
  @Nullable
  private final Pattern compiledValuePattern;
  @Nullable
  private final Counter ruleAppliedCounter;

  public ReportPointDropTagTransformer(final String tag,
                                       @Nullable final String patternMatch,
                                       @Nullable final Counter ruleAppliedCounter) {
    this.compiledTagPattern = Pattern.compile(Preconditions.checkNotNull(tag, "[tag] can't be null"));
    Preconditions.checkArgument(!tag.isEmpty(), "[tag] can't be blank");
    this.compiledValuePattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    this.ruleAppliedCounter = ruleAppliedCounter;
  }

  @Override
  public ReportPoint apply(@NotNull ReportPoint reportPoint) {
    if (reportPoint.getAnnotations() == null || compiledTagPattern == null) {
      return reportPoint;
    }
    Iterator<Map.Entry<String, String>> iterator = reportPoint.getAnnotations().entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      if (compiledTagPattern.matcher(entry.getKey()).matches()) {
        if (compiledValuePattern == null || compiledValuePattern.matcher(entry.getValue()).matches()) {
          iterator.remove();
          if (ruleAppliedCounter != null) {
            ruleAppliedCounter.inc();
          }
        }
      }
    }
    return reportPoint;
  }
}
