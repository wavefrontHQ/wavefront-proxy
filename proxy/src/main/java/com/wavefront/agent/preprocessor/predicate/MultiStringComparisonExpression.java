package com.wavefront.agent.preprocessor.predicate;

import java.util.List;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.ImmutableList;

import wavefront.report.Annotation;
import wavefront.report.ReportPoint;
import wavefront.report.Span;

/**
 * An eval expression that compares a collection of strings
 *
 * @author vasily@wavefront.com
 */
public class MultiStringComparisonExpression implements EvalExpression {

  private final String scope;
  private final StringExpression argument;
  private final boolean all;
  private final BiFunction<String, String, Boolean> cmp;

  private MultiStringComparisonExpression(String scope, StringExpression argument, boolean all,
                                         BiFunction<String, String, Boolean> cmp) {
    this.scope = scope;
    this.argument = argument;
    this.all = all;
    this.cmp = cmp;
  }

  @Override
  public double getValue(Object entity) {
    if (entity == null) {
      return 0;
    }
    List<String> annotations;
    if (entity instanceof Span) {
      if (scope.equals("spanName")) {
        annotations = ImmutableList.of(((Span) entity).getName());
      } else if (scope.equals("sourceName")){
        annotations = ImmutableList.of(((Span) entity).getSource());
      } else {
        annotations = ((Span) entity).getAnnotations().stream().
            filter(a -> a.getKey().equals(scope)).
            map(Annotation::getValue).collect(Collectors.toList());
      }
    } else if (entity instanceof ReportPoint) {
      if (scope.equals("metricName")) {
        annotations = ImmutableList.of(((ReportPoint) entity).getMetric());
      } else if (scope.equals("sourceName")){
        annotations = ImmutableList.of(((ReportPoint) entity).getHost());
      } else {
        annotations = ImmutableList.of(((ReportPoint) entity).getAnnotations().
            getOrDefault(scope, ""));
      }
    } else {
      throw new IllegalArgumentException("Unknown object type: " +
          entity.getClass().getCanonicalName());
    }
    return EvalExpression.asDouble(all ?
        annotations.stream().allMatch(x -> cmp.apply(x, argument.getString(entity))) :
        annotations.stream().anyMatch(x -> cmp.apply(x, argument.getString(entity))));
  }

  public static EvalExpression of(String scope, StringExpression argument, boolean all, String op) {
    switch (op) {
      case "=":
      case "equals":
        return new MultiStringComparisonExpression(scope, argument, all, String::equals);
      case "startsWith":
        return new MultiStringComparisonExpression(scope, argument, all, String::startsWith);
      case "endsWith":
        return new MultiStringComparisonExpression(scope, argument, all, String::endsWith);
      case "contains":
        return new MultiStringComparisonExpression(scope, argument, all, String::contains);
      case "matches":
      case "regexMatch":
        return new MultiStringComparisonExpression(scope, argument, all,
            new CachingPatternMatcher());
      case "equalsIgnoreCase":
        return new MultiStringComparisonExpression(scope, argument, all, String::equalsIgnoreCase);
      case "startsWithIgnoreCase":
        return new MultiStringComparisonExpression(scope, argument, all,
            StringUtils::startsWithIgnoreCase);
      case "endsWithIgnoreCase":
        return new MultiStringComparisonExpression(scope, argument, all,
            StringUtils::endsWithIgnoreCase);
      case "containsIgnoreCase":
        return new MultiStringComparisonExpression(scope, argument, all,
            StringUtils::containsIgnoreCase);
      case "matchesIgnoreCase":
      case "regexMatchIgnoreCase":
        return new MultiStringComparisonExpression(scope, argument, all,
            new CachingPatternMatcher(Pattern.CASE_INSENSITIVE));
      default:
        throw new IllegalArgumentException(op + " is not handled");
    }
  }
}
