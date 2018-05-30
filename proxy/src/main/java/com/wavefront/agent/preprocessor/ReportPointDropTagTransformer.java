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
  private final PreprocessorRuleMetrics ruleMetrics;

  @Deprecated
  public ReportPointDropTagTransformer(final String tag,
                                       @Nullable final String patternMatch,
                                       @Nullable final Counter ruleAppliedCounter) {
    this(tag, patternMatch, new PreprocessorRuleMetrics(ruleAppliedCounter));
  }

  public ReportPointDropTagTransformer(final String tag,
                                       @Nullable final String patternMatch,
                                       final PreprocessorRuleMetrics ruleMetrics) {
    this.compiledTagPattern = Pattern.compile(Preconditions.checkNotNull(tag, "[tag] can't be null"));
    Preconditions.checkArgument(!tag.isEmpty(), "[tag] can't be blank");
    this.compiledValuePattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
  }

  @Override
  public ReportPoint apply(@NotNull ReportPoint reportPoint) {
    long startNanos = ruleMetrics.ruleStart();
    if (reportPoint.getAnnotations() == null || compiledTagPattern == null) {
      ruleMetrics.ruleEnd(startNanos);
      return reportPoint;
    }
    Iterator<Map.Entry<String, String>> iterator = reportPoint.getAnnotations().entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      if (compiledTagPattern.matcher(entry.getKey()).matches()) {
        if (compiledValuePattern == null || compiledValuePattern.matcher(entry.getValue()).matches()) {
          iterator.remove();
          ruleMetrics.incrementRuleAppliedCounter();
        }
      }
    }
    ruleMetrics.ruleEnd(startNanos);
    return reportPoint;
  }
}
