package com.wavefront.agent.preprocessor.predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An expression that returns a string;.
 *
 * @author vasily@wavefront.com.
 */
public interface StringExpression extends Expression {

  /**
   * Get a string value.
   *
   * @param entity entity to get the value from.
   * @return string value
   */
  @Nonnull
  String getString(@Nullable Object entity);
}
