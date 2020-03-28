package com.wavefront.agent.preprocessor.predicate;

import com.wavefront.agent.preprocessor.PreprocessorUtil;

import java.util.List;

import wavefront.report.Span;

/**
 * Predicate mimicking {@link String#endsWith(String)} for Wavefront reportable entities.
 *
 * @author Anil Kodali (akodali@vmware.com).
 */
public class SpanEndsWithPredicate extends ComparisonPredicate<Span>{

  public SpanEndsWithPredicate(String scope, String value) {
    super(scope, value);
  }

  @Override
  public boolean test(Span span) {
    List<String> spanVal = PreprocessorUtil.getReportableEntityComparableValue(scope, span);
    if (spanVal != null) {
      return spanVal.stream().anyMatch(v -> v.endsWith(value));
    }
    return false;
  }
}
