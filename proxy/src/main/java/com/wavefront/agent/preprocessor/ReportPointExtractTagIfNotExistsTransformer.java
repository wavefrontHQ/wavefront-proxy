package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import com.yammer.metrics.core.Counter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import wavefront.report.ReportPoint;

/**
 * Create a point tag by extracting a portion of a metric name, source name or another point tag.
 * If such point tag already exists, the value won't be overwritten.
 *
 * @author vasily@wavefront.com
 * Created 5/18/18
 */
public class ReportPointExtractTagIfNotExistsTransformer extends ReportPointExtractTagTransformer {

  @Deprecated
  public ReportPointExtractTagIfNotExistsTransformer(final String tag,
                                                     final String source,
                                                     final String patternSearch,
                                                     final String patternReplace,
                                                     @Nullable final String patternMatch,
                                                     @Nullable final Counter ruleAppliedCounter) {
    this(tag, source, patternSearch, patternReplace, null, patternMatch,
        new PreprocessorRuleMetrics(ruleAppliedCounter));
  }

  public ReportPointExtractTagIfNotExistsTransformer(final String tag,
                                                     final String source,
                                                     final String patternSearch,
                                                     final String patternReplace,
                                                     @Nullable final String replaceSource,
                                                     @Nullable final String patternMatch,
                                                     final PreprocessorRuleMetrics ruleMetrics) {
    super(tag, source, patternSearch, patternReplace, replaceSource, patternMatch, ruleMetrics);
  }

  @Override
  public ReportPoint apply(@NotNull ReportPoint reportPoint) {
    long startNanos = ruleMetrics.ruleStart();
    if (reportPoint.getAnnotations() == null) {
      reportPoint.setAnnotations(Maps.<String, String>newHashMap());
    }
    if (reportPoint.getAnnotations().get(tag) == null) {
      internalApply(reportPoint);
    }
    ruleMetrics.ruleEnd(startNanos);
    return reportPoint;
  }
}
