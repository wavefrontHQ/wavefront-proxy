package com.wavefront.agent.preprocessor.predicate;

import com.wavefront.agent.preprocessor.PreprocessorUtil;

import java.util.List;
import java.util.regex.Pattern;

import wavefront.report.ReportPoint;
import wavefront.report.Span;

/**
 * @author Anil Kodali (akodali@vmware.com).
 */
public class RegexMatchPredicate<T> extends ComparisonPredicate<T>{

  private final Pattern pattern;
  public RegexMatchPredicate(String scope, String value) {
    super(scope, value);
    this.pattern = Pattern.compile(value);
  }

  @Override
  public boolean test(T t) {
    if (t instanceof ReportPoint) {
      String pointVal = PreprocessorUtil.getReportableEntityComparableValue(scope, (ReportPoint) t);
      if (pointVal != null) {
        return pattern.matcher(pointVal).matches();
      }
    } else if (t instanceof Span) {
      List<String> spanVal = PreprocessorUtil.getReportableEntityComparableValue(scope, (Span) t);
      if (spanVal != null) {
        return spanVal.stream().anyMatch(v -> pattern.matcher(v).matches());
      }
    } else {
      throw new IllegalArgumentException("Invalid Reportable Entity.");
    }
    return false;
  }
}
