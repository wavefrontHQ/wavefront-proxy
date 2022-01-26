package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.annotation.Nonnull;

import wavefront.report.Annotation;
import wavefront.report.ReportLog;

import static com.wavefront.predicates.Util.expandPlaceholders;

/**
 * Create a log tag by extracting a portion of a message, source name or another log tag
 *
 * @author amitw@vmware.com
 */
public class ReportLogExtractTagTransformer implements Function<ReportLog, ReportLog>{

  protected final String tag;
  protected final String input;
  protected final String patternReplace;
  protected final Pattern compiledSearchPattern;
  @Nullable
  protected final Pattern compiledMatchPattern;
  @Nullable
  protected final String patternReplaceInput;
  protected final PreprocessorRuleMetrics ruleMetrics;
  protected final Predicate<ReportLog> v2Predicate;

  public ReportLogExtractTagTransformer(final String tag,
                                        final String input,
                                        final String patternSearch,
                                        final String patternReplace,
                                        @Nullable final String replaceInput,
                                        @Nullable final String patternMatch,
                                        @Nullable final Predicate<ReportLog> v2Predicate,
                                        final PreprocessorRuleMetrics ruleMetrics) {
    this.tag = Preconditions.checkNotNull(tag, "[tag] can't be null");
    this.input = Preconditions.checkNotNull(input, "[input] can't be null");
    this.compiledSearchPattern = Pattern.compile(Preconditions.checkNotNull(patternSearch, "[search] can't be null"));
    this.patternReplace = Preconditions.checkNotNull(patternReplace, "[replace] can't be null");
    Preconditions.checkArgument(!tag.isEmpty(), "[tag] can't be blank");
    Preconditions.checkArgument(!input.isEmpty(), "[input] can't be blank");
    Preconditions.checkArgument(!patternSearch.isEmpty(), "[search] can't be blank");
    this.compiledMatchPattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    this.patternReplaceInput = replaceInput;
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
    this.v2Predicate = v2Predicate != null ? v2Predicate : x -> true;
  }

  protected boolean extractTag(@Nonnull ReportLog reportLog, final String extractFrom) {
    Matcher patternMatcher;
    if (extractFrom == null || (compiledMatchPattern != null && !compiledMatchPattern.matcher(extractFrom).matches())) {
      return false;
    }
    patternMatcher = compiledSearchPattern.matcher(extractFrom);
    if (!patternMatcher.find()) {
      return false;
    }
    String value = patternMatcher.replaceAll(expandPlaceholders(patternReplace, reportLog));
    if (!value.isEmpty()) {
      reportLog.getAnnotations().add(new Annotation(tag, value));
      ruleMetrics.incrementRuleAppliedCounter();
    }
    return true;
  }

  protected void internalApply(@Nonnull ReportLog reportLog) {
    switch (input) {
      case "message":
        if (extractTag(reportLog, reportLog.getMessage()) && patternReplaceInput != null) {
          reportLog.setMessage(compiledSearchPattern.matcher(reportLog.getMessage()).
              replaceAll(expandPlaceholders(patternReplaceInput, reportLog)));
        }
        break;
      case "sourceName":
        if (extractTag(reportLog, reportLog.getHost()) && patternReplaceInput != null) {
          reportLog.setHost(compiledSearchPattern.matcher(reportLog.getHost()).
              replaceAll(expandPlaceholders(patternReplaceInput, reportLog)));
        }
        break;
      default:
        for (Annotation logTagKV : reportLog.getAnnotations()) {
          if (logTagKV.getKey().equals(input)) {
            if (extractTag(reportLog, logTagKV.getValue())) {
              if (patternReplaceInput != null) {
                logTagKV.setValue(compiledSearchPattern.matcher(logTagKV.getValue()).
                    replaceAll(expandPlaceholders(patternReplaceInput, reportLog)));
              }
            }
          }
        }
    }
  }

  @Nullable
  @Override
  public ReportLog apply(@Nullable ReportLog reportLog) {
    if (reportLog == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (!v2Predicate.test(reportLog)) return reportLog;

      internalApply(reportLog);
      return reportLog;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
