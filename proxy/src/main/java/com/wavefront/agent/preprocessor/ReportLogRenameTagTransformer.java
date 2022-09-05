package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import wavefront.report.Annotation;
import wavefront.report.ReportLog;

/**
 * Rename a log tag (optional: if its value matches a regex pattern)
 *
 * @author amitw@vmare.com
 */
public class ReportLogRenameTagTransformer implements Function<ReportLog, ReportLog> {

  private final String tag;
  private final String newTag;
  @Nullable private final Pattern compiledPattern;
  private final PreprocessorRuleMetrics ruleMetrics;
  private final Predicate<ReportLog> v2Predicate;

  public ReportLogRenameTagTransformer(
      final String tag,
      final String newTag,
      @Nullable final String patternMatch,
      @Nullable final Predicate<ReportLog> v2Predicate,
      final PreprocessorRuleMetrics ruleMetrics) {
    this.tag = Preconditions.checkNotNull(tag, "[tag] can't be null");
    this.newTag = Preconditions.checkNotNull(newTag, "[newtag] can't be null");
    Preconditions.checkArgument(!tag.isEmpty(), "[tag] can't be blank");
    Preconditions.checkArgument(!newTag.isEmpty(), "[newtag] can't be blank");
    this.compiledPattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
    this.v2Predicate = v2Predicate != null ? v2Predicate : x -> true;
  }

  @Nullable
  @Override
  public ReportLog apply(@Nullable ReportLog reportLog) {
    if (reportLog == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (!v2Predicate.test(reportLog)) return reportLog;

      Stream<Annotation> stream =
          reportLog.getAnnotations().stream()
              .filter(
                  a ->
                      a.getKey().equals(tag)
                          && (compiledPattern == null
                              || compiledPattern.matcher(a.getValue()).matches()));

      List<Annotation> annotations = stream.collect(Collectors.toList());
      annotations.forEach(a -> a.setKey(newTag));
      if (!annotations.isEmpty()) {
        ruleMetrics.incrementRuleAppliedCounter();
      }

      return reportLog;

    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
