package com.wavefront.agent.preprocessor.predicate;

import com.wavefront.agent.preprocessor.PreprocessorUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import wavefront.report.Span;

/**
 * Predicate to perform a regexMatch for Wavefront reportable entities.
 *
 * @author Anil Kodali (akodali@vmware.com).
 */
public class SpanRegexMatchPredicate extends ComparisonPredicate<Span>{

  private final List<Pattern> pattern;
  public SpanRegexMatchPredicate(String scope, Object value) {
    super(scope, value);
    this.pattern = new ArrayList<>();
    for (String regex : this.value) {
      this.pattern.add(Pattern.compile(regex));
    }
  }

  @Override
  public boolean test(Span span) {
    List<String> spanVal = PreprocessorUtil.getReportableEntityComparableValue(scope, span);
    if (spanVal != null) {
      return spanVal.stream().anyMatch(sv -> pattern.stream().anyMatch(p -> p.matcher(sv).matches()));
    }
    return false;
  }
}
