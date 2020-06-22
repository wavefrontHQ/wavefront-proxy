package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import wavefront.report.ReportPoint;

/**
 * Removes a point tag if its value matches an optional regex pattern (always remove if null)
 *
 * Created by Vasily on 9/13/16.
 */
public class ReportPointDropTagTransformer implements Function<ReportPoint, ReportPoint> {

  @Nonnull
  private final Pattern compiledTagPattern;
  @Nullable
  private final Pattern compiledValuePattern;
  private final PreprocessorRuleMetrics ruleMetrics;
  private final Predicate<ReportPoint> v2Predicate;

  public ReportPointDropTagTransformer(final String tag,
                                       @Nullable final String patternMatch,
                                       @Nullable final Predicate<ReportPoint> v2Predicate,
                                       final PreprocessorRuleMetrics ruleMetrics) {
    this.compiledTagPattern = Pattern.compile(Preconditions.checkNotNull(tag, "[tag] can't be null"));
    Preconditions.checkArgument(!tag.isEmpty(), "[tag] can't be blank");
    this.compiledValuePattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
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
      return reportPoint;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
