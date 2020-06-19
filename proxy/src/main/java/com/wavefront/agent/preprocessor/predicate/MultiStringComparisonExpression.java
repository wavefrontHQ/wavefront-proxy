package com.wavefront.agent.preprocessor.predicate;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import wavefront.report.Annotation;
import wavefront.report.ReportPoint;
import wavefront.report.Span;

public class MultiStringComparisonExpression implements EvalExpression {

  private final String scope;
  private final StringExpression argument;
  private final boolean all;
  private final BiFunction<String, String, Boolean> cmp;

  public MultiStringComparisonExpression(String scope, StringExpression argument, boolean all,
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
}
