package com.wavefront.agent.preprocessor.predicate;

import static com.wavefront.agent.preprocessor.predicate.EvalExpression.asDouble;
import static com.wavefront.agent.preprocessor.predicate.EvalExpression.isTrue;

/**
 * A math expression
 *
 * @author vasily@wavefront.com.
 */
public class MathExpression implements EvalExpression {
  private final EvalExpression left;
  private final EvalExpression right;
  private final String op;

  public MathExpression(EvalExpression left, EvalExpression right, String op) {
    this.left = left;
    this.right = right;
    this.op = op;
  }

  @Override
  public double getValue(Object entity) {
    switch (op) {
      case "and":
        return asDouble(isTrue(left.getValue(entity)) && isTrue(right.getValue(entity)));
      case "or":
        return asDouble(isTrue(left.getValue(entity)) || isTrue(right.getValue(entity)));
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
      case "&":
        return (long) left.getValue(entity) & (long) right.getValue(entity);
      case "|":
        return (long) left.getValue(entity) | (long) right.getValue(entity);
      case "^":
        return (long) left.getValue(entity) ^ (long) right.getValue(entity);
      case ">>":
        return (long) left.getValue(entity) >> (long) right.getValue(entity);
      case ">>>":
        return (long) left.getValue(entity) >>> (long) right.getValue(entity);
      case "<<":
      case "<<<":
        return (long) left.getValue(entity) << (long) right.getValue(entity);
      default:
        throw new IllegalArgumentException("Unknown operator: " + op);
    }
  }
}
