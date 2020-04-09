package com.wavefront.agent.preprocessor.predicate;

import com.wavefront.agent.preprocessor.PreprocessorUtil;

import java.util.List;

import wavefront.report.ReportPoint;
import wavefront.report.Span;

/**
 * Predicate mimicking {@link String#startsWith(String)} for Wavefront reportable entities.
 *
 * @author Anil Kodali (akodali@vmware.com).
 */
public class ReportPointStartsWithPredicate extends ComparisonPredicate<ReportPoint>{

  public ReportPointStartsWithPredicate(String scope, String value) {
    super(scope, value);
  }

  @Override
  public boolean test(ReportPoint reportPoint) {
    String pointVal = PreprocessorUtil.getReportableEntityComparableValue(scope, reportPoint);
    if (pointVal != null) {
      return pointVal.startsWith(value);
    }
    return false;
  }
}
