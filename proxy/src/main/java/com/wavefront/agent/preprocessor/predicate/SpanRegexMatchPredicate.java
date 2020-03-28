package com.wavefront.agent.preprocessor.predicate;

import com.wavefront.agent.preprocessor.PreprocessorUtil;

import java.util.List;
import java.util.regex.Pattern;

import wavefront.report.Span;

/**
 * Predicate to perform a regexMatch for Wavefront reportable entities.
 *
 * @author Anil Kodali (akodali@vmware.com).
 */
public class SpanRegexMatchPredicate extends ComparisonPredicate<Span>{

  private final Pattern pattern;
  public SpanRegexMatchPredicate(String scope, String value) {
    super(scope, value);
    this.pattern = Pattern.compile(value);
  }

  @Override
  public boolean test(Span span) {
    List<String> spanVal = PreprocessorUtil.getReportableEntityComparableValue(scope, span);
    if (spanVal != null) {
      return spanVal.stream().anyMatch(v -> pattern.matcher(v).matches());
    }
    return false;
  }
}
