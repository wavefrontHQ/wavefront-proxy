package com.wavefront.agent.preprocessor.predicate;

import com.wavefront.agent.preprocessor.PreprocessorUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import wavefront.report.Span;

/**
 * An Extension of {@link SpanEqualsPredicate} that takes in a list of values to check.
 *
 * @author Anil Kodali (akodali@vmware.com).
 */
public class SpanInPredicate extends ComparisonPredicate<Span>{

  private final List<String> values;

  public SpanInPredicate(String scope, List<String> values) {
    super(scope, null);
    this.values = Collections.unmodifiableList(values);
  }

  @Override
  public boolean test(Span span) {
    List<String> spanVal = PreprocessorUtil.getReportableEntityComparableValue(scope, span);
    if (spanVal != null) {
      return !Collections.disjoint(spanVal, values);
    }
    return false;
  }
}
