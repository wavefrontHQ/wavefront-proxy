package com.wavefront.agent.preprocessor.predicate;

import java.util.function.Predicate;

/**
 * Base for all comparison predicates.
 *
 * @author Anil Kodali (akodali@vmware.com).
 */
public abstract class ComparisonPredicate<T> implements Predicate<T> {
  protected final String scope;
  protected final String value;

  public ComparisonPredicate(String scope, String value) {
    this.scope = scope;
    this.value = value;
  }
}
