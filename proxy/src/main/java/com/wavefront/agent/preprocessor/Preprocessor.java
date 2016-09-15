package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

/**
 *
 * Created by Vasily on 9/13/16.
 */
public class Preprocessor<T> {

  private final List<Function<T, T>> transformers = new ArrayList<>();
  private final List<AnnotatedPredicate<T>> filters = new ArrayList<>();

  @Nullable
  private String message;

  public T transform(@NotNull T point) {
    for (final Function<T, T> func : transformers) {
      point = func.apply(point);
    }
    return point;
  }

  public boolean filter(@NotNull T point) {
    message = null;
    for (final AnnotatedPredicate<T> predicate : filters) {
      if (!predicate.apply(point)) {
        message = predicate.getMessage(point);
        return false;
      }
    }
    return true;
  }

  public String getLastFilterResult() {
    return message;
  }

  public void addTransformer(Function<T, T> transformer) {
    transformers.add(transformer);
  }

  public void addFilter(AnnotatedPredicate<T> filter) {
    filters.add(filter);
  }

  public void addTransformer(int index, Function<T, T> transformer) {
    transformers.add(index, transformer);
  }

  public void addFilter(int index, AnnotatedPredicate<T> filter) {
    filters.add(index, filter);
  }
}
