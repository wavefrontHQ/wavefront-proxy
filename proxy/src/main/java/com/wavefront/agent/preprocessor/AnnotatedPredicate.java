package com.wavefront.agent.preprocessor;

import java.util.function.Predicate;

import javax.annotation.Nullable;

/**
 * Base for all "filter"-type rules.
 *
 * Created by Vasily on 9/15/16.
 */
public interface AnnotatedPredicate<T> extends Predicate<T> {

  @Override
  default boolean test(T input) {
    return test(input, null);
  }

  boolean test(T input, @Nullable String[] messageHolder);
}
