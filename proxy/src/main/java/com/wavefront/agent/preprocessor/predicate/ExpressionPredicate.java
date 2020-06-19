package com.wavefront.agent.preprocessor.predicate;

import java.util.function.Predicate;

import static com.wavefront.agent.preprocessor.predicate.EvalExpression.isTrue;

public class ExpressionPredicate<T> implements Predicate<T> {
  private final EvalExpression wrapped;

  public ExpressionPredicate(EvalExpression wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public boolean test(T t) {
    return isTrue(wrapped.getValue(t));
  }
}
