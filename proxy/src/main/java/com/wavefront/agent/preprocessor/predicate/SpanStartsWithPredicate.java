package com.wavefront.agent.preprocessor.predicate;

import com.wavefront.agent.preprocessor.PreprocessorUtil;

import java.util.List;

import wavefront.report.Span;

/**
 * Predicate mimicking {@link String#startsWith(String)} for Wavefront reportable entities.
 *
 * @author Anil Kodali (akodali@vmware.com).
 */
public class SpanStartsWithPredicate extends ComparisonPredicate<Span>{

  public SpanStartsWithPredicate(String scope, Object value) {
    super(scope, value);
  }

  @Override
  public boolean test(Span span) {
    List<String> spanVal = PreprocessorUtil.getReportableEntityComparableValue(scope, span);
    if (spanVal != null) {
      return spanVal.stream().anyMatch(sv -> value.stream().anyMatch(v -> sv.startsWith(v)));
    }
    return false;
  }
}
