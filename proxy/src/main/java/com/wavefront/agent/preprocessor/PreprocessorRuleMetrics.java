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
  @Nullable
  private final Counter ruleCheckedCounter;

  public PreprocessorRuleMetrics(@Nullable Counter ruleAppliedCounter, @Nullable Counter ruleCpuTimeNanosCounter,
                                 @Nullable Counter ruleCheckedCounter) {
    this.ruleAppliedCounter = ruleAppliedCounter;
    this.ruleCpuTimeNanosCounter = ruleCpuTimeNanosCounter;
    this.ruleCheckedCounter = ruleCheckedCounter;
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
   * Measure rule execution time and add it to ruleCpuTimeNanosCounter (if available)
   *
   * @param ruleStartTime rule start time
   */
  public void ruleEnd(long ruleStartTime) {
    if (this.ruleCpuTimeNanosCounter != null) {
      this.ruleCpuTimeNanosCounter.inc(System.nanoTime() - ruleStartTime);
    }
  }


  /**
   * Mark rule start time, increment ruleCheckedCounter (if available) by 1
   *
   * @return start time in nanos
   */
  public long ruleStart() {
    if (this.ruleCheckedCounter != null) {
      this.ruleCheckedCounter.inc();
    }
    return System.nanoTime();
  }
}
