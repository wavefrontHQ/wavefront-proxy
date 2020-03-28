package com.wavefront.agent.preprocessor.predicate;

import com.wavefront.agent.preprocessor.PreprocessorUtil;

import java.util.regex.Pattern;

import wavefront.report.ReportPoint;

/**
 * Predicate to perform a regexMatch for Wavefront reportable entities.
 *
 * @author Anil Kodali (akodali@vmware.com).
 */
public class ReportPointRegexMatchPredicate extends ComparisonPredicate<ReportPoint>{

  private final Pattern pattern;
  public ReportPointRegexMatchPredicate(String scope, String value) {
    super(scope, value);
    this.pattern = Pattern.compile(value);
  }

  @Override
  public boolean test(ReportPoint reportPoint) {
    String pointVal = PreprocessorUtil.getReportableEntityComparableValue(scope, reportPoint);
    if (pointVal != null) {
      return pattern.matcher(pointVal).matches();
    }
    return false;
  }
}
