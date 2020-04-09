package com.wavefront.agent.preprocessor.predicate;

import com.wavefront.agent.preprocessor.PreprocessorUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import wavefront.report.ReportPoint;

/**
 * An Extension of {@link ReportPointEqualsPredicate} that takes in a range of comma separated values to check.
 *
 * @author Anil Kodali (akodali@vmware.com).
 */
public class ReportPointInPredicate extends ComparisonPredicate<ReportPoint>{

  private final List<String> values;

  public ReportPointInPredicate(String scope, List<String> values) {
    super(scope, null);
    this.values = Collections.unmodifiableList(values);
  }

  @Override
  public boolean test(ReportPoint reportPoint) {
    String pointVal = PreprocessorUtil.getReportableEntityComparableValue(scope, reportPoint);
    if (pointVal != null) {
      return values.contains(pointVal);
    }
    return false;
  }
}
