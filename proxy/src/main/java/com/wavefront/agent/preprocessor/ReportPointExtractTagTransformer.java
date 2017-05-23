package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import com.yammer.metrics.core.Counter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import sunnylabs.report.ReportPoint;

/**
 * Create a point tag by extracting a portion of a metric name, source name or another point tag
 *
 * Created by Vasily on 11/15/16.
 */
public class ReportPointExtractTagTransformer implements Function<ReportPoint, ReportPoint>{

  private final String tag;
  private final String source;
  private final String patternReplace;
  private final Pattern compiledSearchPattern;
  @Nullable
  private final Pattern compiledMatchPattern;
  @Nullable
  private final Counter ruleAppliedCounter;

  public ReportPointExtractTagTransformer(final String tag,
                                          final String source,
                                          final String patternSearch,
                                          final String patternReplace,
                                          @Nullable final String patternMatch,
                                          @Nullable final Counter ruleAppliedCounter) {
    this.tag = Preconditions.checkNotNull(tag, "[tag] can't be null");
    this.source = Preconditions.checkNotNull(source, "[source] can't be null");
    this.compiledSearchPattern = Pattern.compile(Preconditions.checkNotNull(patternSearch, "[search] can't be null"));
    this.patternReplace = Preconditions.checkNotNull(patternReplace, "[replace] can't be null");
    Preconditions.checkArgument(!tag.isEmpty(), "[tag] can't be blank");
    Preconditions.checkArgument(!source.isEmpty(), "[source] can't be blank");
    Preconditions.checkArgument(!patternSearch.isEmpty(), "[search] can't be blank");
    this.compiledMatchPattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    this.ruleAppliedCounter = ruleAppliedCounter;
  }

  private void extractTag(@NotNull ReportPoint reportPoint, final String extractFrom) {
    Matcher patternMatcher;
    if (compiledMatchPattern != null && !compiledMatchPattern.matcher(extractFrom).matches()) {
      return;
    }
    patternMatcher = compiledSearchPattern.matcher(extractFrom);
    if (patternMatcher.find()) {
      if (reportPoint.getAnnotations() == null) {
        reportPoint.setAnnotations(Maps.<String, String>newHashMap());
      }
      String value = patternMatcher.replaceAll(patternReplace);
      if (!value.isEmpty()) {
        reportPoint.getAnnotations().put(tag, value);
        if (ruleAppliedCounter != null) {
          ruleAppliedCounter.inc();
        }
      }
    }
  }

  @Override
  public ReportPoint apply(@NotNull ReportPoint reportPoint) {
    switch (source) {
      case "metricName":
        extractTag(reportPoint, reportPoint.getMetric());
        break;
      case "sourceName":
        extractTag(reportPoint, reportPoint.getHost());
        break;
      default:
        if (reportPoint.getAnnotations() != null) {
          extractTag(reportPoint, reportPoint.getAnnotations().get(source));
        }
    }
    return reportPoint;
  }
}
