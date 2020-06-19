package com.wavefront.agent.preprocessor.predicate;

import static com.wavefront.agent.preprocessor.predicate.EvalExpression.asDouble;

public class MathExpression implements EvalExpression {
  private final EvalExpression left;
  private final EvalExpression right;
  private final String op;

  public MathExpression(EvalExpression left, EvalExpression right, String op) {
    this.left = left;
    this.right = right;
    this.op = op.replace(" ", "");
  }

  @Override
  public double getValue(Object entity) {
    switch (op) {
      case "+":
        return left.getValue(entity) + right.getValue(entity);
      case "-":
        return left.getValue(entity) - right.getValue(entity);
      case "*":
        return left.getValue(entity) * right.getValue(entity);
      case "/":
        return left.getValue(entity) / right.getValue(entity);
      case "%":
        return left.getValue(entity) % right.getValue(entity);
      case "=":
        return asDouble(left.getValue(entity) == right.getValue(entity));
      case ">":
        return asDouble(left.getValue(entity) > right.getValue(entity));
      case "<":
        return asDouble(left.getValue(entity) < right.getValue(entity));
      case "<=":
        return asDouble(left.getValue(entity) <= right.getValue(entity));
      case ">=":
        return asDouble(left.getValue(entity) >= right.getValue(entity));
      case "!=":
        return asDouble(left.getValue(entity) != right.getValue(entity));
      default:
        throw new IllegalArgumentException("Unknown operator: " + op);
    }
  }
}
