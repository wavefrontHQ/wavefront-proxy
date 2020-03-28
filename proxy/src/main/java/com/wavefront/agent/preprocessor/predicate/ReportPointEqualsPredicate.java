package com.wavefront.agent.preprocessor.predicate;

import com.wavefront.agent.preprocessor.PreprocessorUtil;

import wavefront.report.ReportPoint;

/**
 * Predicate mimicking {@link String#equals(Object)} for Wavefront reportable entities.
 *
 * @author Anil Kodali (akodali@vmware.com).
 */
public class ReportPointEqualsPredicate extends ComparisonPredicate<ReportPoint>{

  public ReportPointEqualsPredicate(String scope, String value) {
    super(scope, value);
  }

  @Override
  public boolean test(ReportPoint reportPoint) {
    String pointVal = PreprocessorUtil.getReportableEntityComparableValue(scope, reportPoint);
    if (pointVal != null) {
      return pointVal.equals(value);
    }
    return false;
  }
}
