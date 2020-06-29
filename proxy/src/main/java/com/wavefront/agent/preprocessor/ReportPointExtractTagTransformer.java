package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.annotation.Nonnull;

import wavefront.report.ReportPoint;

import static com.wavefront.predicates.Util.expandPlaceholders;

/**
 * Create a point tag by extracting a portion of a metric name, source name or another point tag
 *
 * Created by Vasily on 11/15/16.
 */
public class ReportPointExtractTagTransformer implements Function<ReportPoint, ReportPoint>{

  protected final String tag;
  protected final String source;
  protected final String patternReplace;
  protected final Pattern compiledSearchPattern;
  @Nullable
  protected final Pattern compiledMatchPattern;
  @Nullable
  protected final String patternReplaceSource;
  protected final PreprocessorRuleMetrics ruleMetrics;
  protected final Predicate<ReportPoint> v2Predicate;

  public ReportPointExtractTagTransformer(final String tag,
                                          final String source,
                                          final String patternSearch,
                                          final String patternReplace,
                                          @Nullable final String replaceSource,
                                          @Nullable final String patternMatch,
                                          @Nullable final Predicate<ReportPoint> v2Predicate,
                                          final PreprocessorRuleMetrics ruleMetrics) {
    this.tag = Preconditions.checkNotNull(tag, "[tag] can't be null");
    this.source = Preconditions.checkNotNull(source, "[source] can't be null");
    this.compiledSearchPattern = Pattern.compile(Preconditions.checkNotNull(patternSearch, "[search] can't be null"));
    this.patternReplace = Preconditions.checkNotNull(patternReplace, "[replace] can't be null");
    Preconditions.checkArgument(!tag.isEmpty(), "[tag] can't be blank");
    Preconditions.checkArgument(!source.isEmpty(), "[source] can't be blank");
    Preconditions.checkArgument(!patternSearch.isEmpty(), "[search] can't be blank");
    this.compiledMatchPattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    this.patternReplaceSource = replaceSource;
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
    this.v2Predicate = v2Predicate != null ? v2Predicate : x -> true;
  }

  protected boolean extractTag(@Nonnull ReportPoint reportPoint, final String extractFrom) {
    Matcher patternMatcher;
    if (extractFrom == null || (compiledMatchPattern != null && !compiledMatchPattern.matcher(extractFrom).matches())) {
      return false;
    }
    patternMatcher = compiledSearchPattern.matcher(extractFrom);
    if (!patternMatcher.find()) {
      return false;
    }
    String value = patternMatcher.replaceAll(expandPlaceholders(patternReplace, reportPoint));
    if (!value.isEmpty()) {
      reportPoint.getAnnotations().put(tag, value);
      ruleMetrics.incrementRuleAppliedCounter();
    }
    return true;
  }

  protected void internalApply(@Nonnull ReportPoint reportPoint) {
    switch (source) {
      case "metricName":
        if (extractTag(reportPoint, reportPoint.getMetric()) && patternReplaceSource != null) {
          reportPoint.setMetric(compiledSearchPattern.matcher(reportPoint.getMetric()).
              replaceAll(expandPlaceholders(patternReplaceSource, reportPoint)));
        }
        break;
      case "sourceName":
        if (extractTag(reportPoint, reportPoint.getHost()) && patternReplaceSource != null) {
          reportPoint.setHost(compiledSearchPattern.matcher(reportPoint.getHost()).
              replaceAll(expandPlaceholders(patternReplaceSource, reportPoint)));
        }
        break;
      default:
        if (reportPoint.getAnnotations() != null && reportPoint.getAnnotations().get(source) != null) {
          if (extractTag(reportPoint, reportPoint.getAnnotations().get(source)) && patternReplaceSource != null) {
            reportPoint.getAnnotations().put(source,
                compiledSearchPattern.matcher(reportPoint.getAnnotations().get(source)).
                    replaceAll(expandPlaceholders(patternReplaceSource, reportPoint)));
          }
        }
    }
  }

  @Nullable
  @Override
  public ReportPoint apply(@Nullable ReportPoint reportPoint) {
    if (reportPoint == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (!v2Predicate.test(reportPoint)) return reportPoint;

      internalApply(reportPoint);
      return reportPoint;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
