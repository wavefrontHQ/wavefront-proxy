package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;

/**
 * Created by Vasily on 9/13/16.
 */
public class PointPreprocessor<T> {
  private final List<Function<T, T>> transformers = new ArrayList<>();
  private final List<Predicate<T>> filters = new ArrayList<>();

  public T transform(@NotNull T point) {
    for (final Function<T, T> func : transformers) {
      point = func.apply(point);
    }
    return point;
  }

  public boolean filter(@NotNull T point) {
    for (final Predicate<T> predicate : filters) {
      if (!predicate.apply(point)) {
        return false;
      }
    }
    return true;
  }

  public void addTransformer(Function<T, T> transformer) {
    transformers.add(transformer);
  }

  public void addFilter(Predicate<T> filter) {
    filters.add(filter);
  }

  public void addTransformer(int index, Function<T, T> transformer) {
    transformers.add(index, transformer);
  }

  public void addFilter(int index, Predicate<T> filter) {
    filters.add(index, filter);
  }

}
