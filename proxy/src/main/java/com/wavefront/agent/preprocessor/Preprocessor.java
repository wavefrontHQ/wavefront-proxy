package com.wavefront.agent.preprocessor;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import javax.annotation.Nullable;
import javax.annotation.Nonnull;

/**
 * Generic container class for storing transformation and filter rules
 *
 * Created by Vasily on 9/13/16.
 */
public class Preprocessor<T> {

  private final List<Function<T, T>> transformers;
  private final List<AnnotatedPredicate<T>> filters;

  public Preprocessor() {
    this(new ArrayList<>(), new ArrayList<>());
  }

  public Preprocessor(List<Function<T, T>> transformers, List<AnnotatedPredicate<T>> filters) {
    this.transformers = transformers;
    this.filters = filters;
  }

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
    return filter(item, null);
  }

  /**
   * Apply all filter predicates sequentially, stop at the first "false" result
   * @param item          item to apply predicates to
   * @param messageHolder container to store additional output from predicate filters
   * @return true if all predicates returned "true"
   */
  public boolean filter(@Nonnull T item, @Nullable String[] messageHolder) {
    if (messageHolder != null) {
      // empty the container to prevent previous call's results from leaking into the current one
      messageHolder[0] = null;
    }
    for (final AnnotatedPredicate<T> predicate : filters) {
      if (!predicate.test(item, messageHolder)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check all filter rules as an immutable list
   * @return filter rules
   */
  public List<AnnotatedPredicate<T>> getFilters() {
    return ImmutableList.copyOf(filters);
  }

  /**
   * Get all transformer rules as an immutable list
   * @return transformer rules
   */
  public List<Function<T, T>> getTransformers() {
    return ImmutableList.copyOf(transformers);
  }

  /**
   * Create a new preprocessor with rules merged from this and another preprocessor.
   *
   * @param other preprocessor to merge with.
   * @return merged preprocessor.
   */
  public Preprocessor<T> merge(Preprocessor<T> other) {
    Preprocessor<T> result = new Preprocessor<>();
    this.getTransformers().forEach(result::addTransformer);
    this.getFilters().forEach(result::addFilter);
    other.getTransformers().forEach(result::addTransformer);
    other.getFilters().forEach(result::addFilter);
    return result;
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
