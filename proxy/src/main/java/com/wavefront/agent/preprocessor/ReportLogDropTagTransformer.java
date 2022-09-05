package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import wavefront.report.Annotation;
import wavefront.report.ReportLog;

/**
 * Removes a log tag if its value matches an optional regex pattern (always remove if null)
 *
 * @author amitw@vmware.com
 */
public class ReportLogDropTagTransformer implements Function<ReportLog, ReportLog> {

  @Nonnull private final Pattern compiledTagPattern;
  @Nullable private final Pattern compiledValuePattern;
  private final PreprocessorRuleMetrics ruleMetrics;
  private final Predicate<ReportLog> v2Predicate;

  public ReportLogDropTagTransformer(
      final String tag,
      @Nullable final String patternMatch,
      @Nullable final Predicate<ReportLog> v2Predicate,
      final PreprocessorRuleMetrics ruleMetrics) {
    this.compiledTagPattern =
        Pattern.compile(Preconditions.checkNotNull(tag, "[tag] can't be null"));
    Preconditions.checkArgument(!tag.isEmpty(), "[tag] can't be blank");
    this.compiledValuePattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
    this.v2Predicate = v2Predicate != null ? v2Predicate : x -> true;
  }

  @Nullable
  @Override
  public ReportLog apply(@Nullable ReportLog reportlog) {
    if (reportlog == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (!v2Predicate.test(reportlog)) return reportlog;

      List<Annotation> annotations = new ArrayList<>(reportlog.getAnnotations());
      Iterator<Annotation> iterator = annotations.iterator();
      boolean changed = false;
      while (iterator.hasNext()) {
        Annotation entry = iterator.next();
        if (compiledTagPattern.matcher(entry.getKey()).matches()
            && (compiledValuePattern == null
                || compiledValuePattern.matcher(entry.getValue()).matches())) {
          changed = true;
          iterator.remove();
          ruleMetrics.incrementRuleAppliedCounter();
        }
      }
      if (changed) {
        reportlog.setAnnotations(annotations);
      }
      return reportlog;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
