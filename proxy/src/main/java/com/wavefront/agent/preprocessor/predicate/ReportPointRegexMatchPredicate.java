package com.wavefront.agent.preprocessor.predicate;

import com.google.common.base.Preconditions;

import com.wavefront.agent.preprocessor.PreprocessorUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import wavefront.report.ReportPoint;

/**
 * Predicate to perform a regexMatch for Wavefront reportable entities.
 *
 * @author Anil Kodali (akodali@vmware.com).
 */
public class ReportPointRegexMatchPredicate extends ComparisonPredicate<ReportPoint>{

  private final List<Pattern> pattern;
  public ReportPointRegexMatchPredicate(String scope, Object value) {
    super(scope, value);
    this.pattern = new ArrayList<>();
    for (String regex : this.value) {
      this.pattern.add(Pattern.compile(regex));
    }
  }

  @Override
  public boolean test(ReportPoint reportPoint) {
    String pointVal = PreprocessorUtil.getReportableEntityComparableValue(scope, reportPoint);
    if (pointVal != null) {
      return pattern.stream().anyMatch(x -> x.matcher(pointVal).matches());
    }
    return false;
  }
}
