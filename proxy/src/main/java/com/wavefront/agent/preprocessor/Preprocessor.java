package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.Nonnull;

/**
 * Generic container class for storing transformation and filter rules
 *
 * Created by Vasily on 9/13/16.
 */
public class Preprocessor<T> {

  private final List<Function<T, T>> transformers = new ArrayList<>();
  private final List<AnnotatedPredicate<T>> filters = new ArrayList<>();

  @Nullable
  private String message;

  /**
   * Apply all transformation rules sequentially
   * @param item input point
   * @return transformed point
   */
  public T transform(@Nonnull T item) {
    for (final Function<T, T> func : transformers) {
      item = func.apply(item);
    }
    return item;
  }

  /**
   * Apply all filter predicates sequentially, stop at the first "false" result
   * @param item item to apply predicates to
   * @return true if all predicates returned "true"
   */
  public boolean filter(@Nonnull T item) {
    message = null;
    for (final AnnotatedPredicate<T> predicate : filters) {
      if (!predicate.apply(item)) {
        message = predicate.getMessage(item);
        return false;
      }
    }
    return true;
  }

  /**
   * Check if any filters are registered
   * @return true if it has at least one filter
   */
  public boolean hasFilters() {
    return !filters.isEmpty();
  }

  /**
   * Check if any transformation rules are registered
   * @return true if it has at least one transformer
   */
  public boolean hasTransformers() {
    return !transformers.isEmpty();
  }

  /**
   * Get the detailed message, if available, with the result of the last filter() operation
   * @return message
   */
  @Nullable
  public String getLastFilterResult() {
    return message;
  }

  /**
   * Register a transformation rule
   * @param transformer rule
   */
  public void addTransformer(Function<T, T> transformer) {
    transformers.add(transformer);
  }

  /**
   * Register a filter rule
   * @param filter rule
   */
  public void addFilter(AnnotatedPredicate<T> filter) {
    filters.add(filter);
  }

  /**
   * Register a transformation rule and place it at a specific index
   * @param index zero-based index
   * @param transformer rule
   */
  public void addTransformer(int index, Function<T, T> transformer) {
    transformers.add(index, transformer);
  }

  /**
   * Register a filter rule and place it at a specific index
   * @param index zero-based index
   * @param filter rule
   */
  public void addFilter(int index, AnnotatedPredicate<T> filter) {
    filters.add(index, filter);
  }
}
