package com.wavefront.agent.preprocessor.predicate;

/**
 * An expression that returns a numeric value.
 *
 * @author vasily@wavefront.com.
 */
public interface EvalExpression extends Expression {

  /**
   * Get a double value.
   *
   * @param entity entity to get the value from.
   * @return double value
   */
  double getValue(Object entity);

  /**
   * Helper method to convert a double value into a boolean.
   *
   * @param value value to evaluate
   * @return true if value is close to 0 (within 1e-6)
   */
  static boolean isTrue(double value) {
    return Math.abs(value) > 1e-6;
  }

  /**
   * Helper method to convert a boolean value into a double.
   *
   * @param value value to evaluate
   * @return 1.0 for true, 0.0 for false.
   */
  static double asDouble(boolean value) { return value ? 1 : 0; }
}
