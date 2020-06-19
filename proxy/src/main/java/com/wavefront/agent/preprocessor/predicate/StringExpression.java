package com.wavefront.agent.preprocessor.predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface StringExpression extends Expression {
  @Nonnull
  String getString(@Nullable Object entity);
}
