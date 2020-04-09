package com.wavefront.agent.preprocessor.predicate;

import com.wavefront.agent.preprocessor.PreprocessorUtil;

import java.util.List;

import wavefront.report.Span;

/**
 * Predicate mimicking {@link String#equals(Object)} for Wavefront reportable entities.
 *
 * @author Anil Kodali (akodali@vmware.com).
 */
public class SpanEqualsPredicate extends ComparisonPredicate<Span>{

  public SpanEqualsPredicate(String scope, Object value) {
    super(scope, value);
  }

  @Override
  public boolean test(Span span) {
    List<String> spanVal = PreprocessorUtil.getReportableEntityComparableValue(scope, span);
    if (spanVal != null) {
      return value.stream().anyMatch(v -> spanVal.contains(v));
    }
    return false;
  }
}
