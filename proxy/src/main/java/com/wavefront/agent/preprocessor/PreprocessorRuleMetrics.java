package com.wavefront.agent.preprocessor;

import com.yammer.metrics.core.Counter;

import javax.annotation.Nullable;

/**
 * A helper class for instrumenting preprocessor rules.
 * Tracks two counters: number of times the rule has been successfully applied, and counter of CPU time (nanos)
 * spent on applying the rule to troubleshoot possible performance issues.
 *
 * @author vasily@wavefront.com
 */
public class PreprocessorRuleMetrics {
  @Nullable
  private final Counter ruleAppliedCounter;
  @Nullable
  private final Counter ruleCpuTimeNanosCounter;

  public PreprocessorRuleMetrics(@Nullable Counter ruleAppliedCounter, @Nullable Counter ruleCpuTimeNanosCounter) {
    this.ruleAppliedCounter = ruleAppliedCounter;
    this.ruleCpuTimeNanosCounter = ruleCpuTimeNanosCounter;
  }

  public PreprocessorRuleMetrics(@Nullable Counter ruleAppliedCounter) {
    this(ruleAppliedCounter, null);
  }

  /**
   * Increment ruleAppliedCounter (if available) by 1
   */
  public void incrementRuleAppliedCounter() {
    if (this.ruleAppliedCounter != null) {
      this.ruleAppliedCounter.inc();
    }
  }

  /**
   * Increment ruleCpuTimeNanosCounter (if available) by {@code n}
   *
   * @param n the amount by which the counter will be increased
   */
  public void countCpuNanos(long n) {
    if (this.ruleCpuTimeNanosCounter != null) {
      this.ruleCpuTimeNanosCounter.inc(n);
    }
  }
}
