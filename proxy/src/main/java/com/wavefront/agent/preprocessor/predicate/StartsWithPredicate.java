package com.wavefront.agent.preprocessor.predicate;

import com.wavefront.agent.preprocessor.PreprocessorUtil;

import java.util.List;

import wavefront.report.ReportPoint;
import wavefront.report.Span;

/**
 * @author Anil Kodali (akodali@vmware.com).
 */
public class StartsWithPredicate<T> extends ComparisonPredicate<T>{

  public StartsWithPredicate(String scope, String value) {
    super(scope, value);
  }

  @Override
  public boolean test(T t) {
    if (t instanceof ReportPoint) {
      String pointVal = PreprocessorUtil.getReportableEntityComparableValue(scope, (ReportPoint) t);
      if (pointVal != null) {
        return pointVal.startsWith(value);
      }
    } else if (t instanceof Span) {
      List<String> spanVal = PreprocessorUtil.getReportableEntityComparableValue(scope, (Span) t);
      if (spanVal != null) {
        return spanVal.stream().anyMatch(v -> v.startsWith(value));
      }
    } else {
      throw new IllegalArgumentException("Invalid Reportable Entity.");
    }
    return false;
  }
}
