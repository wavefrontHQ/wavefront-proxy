package com.wavefront.agent.preprocessor.predicate;

import com.wavefront.agent.preprocessor.PreprocessorUtil;

import java.util.List;

import wavefront.report.Span;

/**
 * Predicate mimicking {@link String#contains(CharSequence)} for Wavefront reportable entities.
 *
 * @author Anil Kodali (akodali@vmware.com).
 */
public class SpanContainsPredicate extends ComparisonPredicate<Span>{

  public SpanContainsPredicate(String scope, String value) {
    super(scope, value);
  }

  @Override
  public boolean test(Span span) {
    List<String> spanVal = PreprocessorUtil.getReportableEntityComparableValue(scope, span);
    if (spanVal != null) {
      return spanVal.stream().anyMatch(v -> v.contains(value));
    }
    return false;
  }
}
