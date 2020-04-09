package com.wavefront.agent.preprocessor.predicate;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

/**
 * Base for all comparison predicates.
 *
 * @author Anil Kodali (akodali@vmware.com).
 */
public abstract class ComparisonPredicate<T> implements Predicate<T> {
  protected final String scope;
  protected final List<String> value;

  public ComparisonPredicate(String scope, Object value) {
    this.scope = scope;
    Preconditions.checkArgument(value instanceof List || value instanceof String,
        "Argument [value] should be of type [string] or [list].");
    this.value =  (value instanceof List) ? ((List) value) :
        Collections.unmodifiableList(Arrays.asList((String) value));
  }
}
