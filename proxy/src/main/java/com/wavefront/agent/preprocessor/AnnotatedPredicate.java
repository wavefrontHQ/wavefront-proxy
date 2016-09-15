package com.wavefront.agent.preprocessor;

import com.google.common.base.Predicate;

import javax.annotation.Nullable;

/**
 * This is the base class for all "filter"-type rules.
 *
 * Created by Vasily on 9/15/16.
 */
public abstract class AnnotatedPredicate<T> implements Predicate<T> {

  public abstract boolean apply(T input);

  /**
   * If special handling is needed based on the result of apply(), override getMessage() to return more details
   */
  public @Nullable String getMessage(T input) {
    return null;
  }
}
