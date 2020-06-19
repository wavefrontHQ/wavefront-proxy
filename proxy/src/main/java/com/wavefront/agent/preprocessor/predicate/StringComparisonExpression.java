package com.wavefront.agent.preprocessor.predicate;

import java.util.function.BiFunction;

import static com.wavefront.agent.preprocessor.predicate.EvalExpression.asDouble;

public class StringComparisonExpression implements EvalExpression {

  private final StringExpression left;
  private final StringExpression right;
  private final BiFunction<String, String, Boolean> func;

  public StringComparisonExpression(StringExpression left,
                                    StringExpression right,
                                    BiFunction<String, String, Boolean> func) {
    this.left = left;
    this.right = right;
    this.func = func;
  }

  @Override
  public double getValue(Object entity) {
    return asDouble(func.apply(left.getString(entity), right.getString(entity)));
  }
}
