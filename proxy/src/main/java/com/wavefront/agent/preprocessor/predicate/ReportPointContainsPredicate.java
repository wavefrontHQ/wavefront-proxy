package com.wavefront.agent.preprocessor.predicate;

import com.wavefront.agent.preprocessor.PreprocessorUtil;

import wavefront.report.ReportPoint;

/**
 * Predicate mimicking {@link String#contains(java.lang.CharSequence)} for Wavefront reportable entities.
 *
 * @author Anil Kodali (akodali@vmware.com).
 */
public class ReportPointContainsPredicate extends ComparisonPredicate<ReportPoint>{

  public ReportPointContainsPredicate(String scope, String value) {
    super(scope, value);
  }

  @Override
  public boolean test(ReportPoint reportPoint) {
    String pointVal = PreprocessorUtil.getReportableEntityComparableValue(scope, reportPoint);
    if (pointVal != null) {
      return pointVal.contains(value);
    }
    return false;
  }
}
