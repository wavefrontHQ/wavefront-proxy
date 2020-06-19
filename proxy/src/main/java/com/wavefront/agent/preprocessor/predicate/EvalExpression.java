package com.wavefront.agent.preprocessor.predicate;

/**
 * A numeric expression that can also be a value getter.
 *
 * @author vasily@wavefront.com.
 */
public interface EvalExpression extends Expression {
  double getValue(Object entity);

  static boolean isTrue(double value) {
    return Math.abs(value) > 1e-6;
  }

  static double asDouble(boolean value) { return value ? 1 : 0; }
}
