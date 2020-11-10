package com.wavefront.agent.preprocessor;

import javax.annotation.Nullable;
import java.util.function.Predicate;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

/**
 * A no-op rule that simply counts points or spans. Optionally, can count only
 * points/spans matching the {@code if} predicate.
 *
 * @author vasily@wavefront.com
 */
public class CountTransformer<T> implements Function<T, T> {

  private final PreprocessorRuleMetrics ruleMetrics;
  private final Predicate<T> v2Predicate;

  public CountTransformer(@Nullable final Predicate<T> v2Predicate,
                          final PreprocessorRuleMetrics ruleMetrics) {
    Preconditions.checkNotNull(ruleMetrics, "PreprocessorRuleMetrics can't be null");
    this.ruleMetrics = ruleMetrics;
    this.v2Predicate = v2Predicate != null ? v2Predicate : x -> true;
  }

  @Nullable
  @Override
  public T apply(@Nullable T input) {
    if (input == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    try {
      if (v2Predicate.test(input)) {
        ruleMetrics.incrementRuleAppliedCounter();
      }
      return input;
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
  }
}
