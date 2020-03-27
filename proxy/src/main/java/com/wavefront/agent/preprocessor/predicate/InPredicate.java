package com.wavefront.agent.preprocessor.predicate;

import com.wavefront.agent.preprocessor.PreprocessorUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import wavefront.report.ReportPoint;
import wavefront.report.Span;

/**
 * An Extension of {@link EqualsPredicate} that takes in a range of comma separated values to check.
 *
 * @author Anil Kodali (akodali@vmware.com).
 */
public class InPredicate<T> extends ComparisonPredicate<T>{

  private final List<String> values;

  public InPredicate(String scope, String value) {
    super(scope, value);
    this.values = Arrays.asList(value.trim().split("\\s*,\\s*"));
  }

  @Override
  public boolean test(T t) {
    if (t instanceof ReportPoint) {
      String pointVal = PreprocessorUtil.getReportableEntityComparableValue(scope, (ReportPoint) t);
      if (pointVal != null) {
        return values.contains(pointVal);
      }
    } else if (t instanceof Span) {
      List<String> spanVal = PreprocessorUtil.getReportableEntityComparableValue(scope, (Span) t);
      if (spanVal != null) {
        return !Collections.disjoint(spanVal, values);
      }
    } else {
      throw new IllegalArgumentException("Invalid Reportable Entity.");
    }
    return false;
  }
}
