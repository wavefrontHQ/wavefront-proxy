package com.wavefront.agent.preprocessor.predicate;

/**
 * Aggregation method for predicate matchers. Translates to allMatch(), anyMatch(), noneMatch().
 *
 * @author vasily@wavefront.com.
 */
enum PredicateMatchOp {
  ALL, ANY, NONE;

  static PredicateMatchOp fromString(String input) {
    for (PredicateMatchOp match : PredicateMatchOp.values()) {
      if (match.name().equalsIgnoreCase(input)) {
        return match;
      }
    }
    throw new IllegalArgumentException(input + " is not a valid match!");
  }
}
