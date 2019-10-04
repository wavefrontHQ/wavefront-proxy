package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.wavefront.common.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;
import wavefront.report.Annotation;
import wavefront.report.Span;

/**
 * Sanitize spans (e.g., span source and tag keys) according to the same rules that are applied at
 * the SDK-level.
 *
 * @author Han Zhang (zhanghan@vmware.com)
 */
public class SpanSanitizeTransformer implements Function<Span, Span> {
  private final PreprocessorRuleMetrics ruleMetrics;

  public SpanSanitizeTransformer(final PreprocessorRuleMetrics ruleMetrics) {
    this.ruleMetrics = ruleMetrics;
  }

  @Nullable
  @Override
  public Span apply(@Nullable Span span) {
    long startNanos = ruleMetrics.ruleStart();
    boolean ruleApplied = false;
    Pair<String, Boolean> sanitizedSource = sanitize(span.getSource());
    if (sanitizedSource._2) {
      span.setSource(sanitizedSource._1);
      ruleApplied = true;
    }
    if (span.getAnnotations() != null) {
      for (Annotation a : span.getAnnotations()) {
        Pair<String, Boolean> sanitizedKey = sanitize(a.getKey());
        if (sanitizedKey._2) {
          a.setKey(sanitizedKey._1);
          ruleApplied = true;
        }
      }
    }
    if (ruleApplied) {
      ruleMetrics.incrementRuleAppliedCounter();
    }
    ruleMetrics.ruleEnd(startNanos);
    return span;
  }

  /**
   * Sanitize a string so that every invalid character is replaced with a dash.
   *
   * @param s The string to sanitize.
   * @return A {@link Pair} containing the resulting string and a boolean indicating whether any
   * sanitization was applied.
   */
  private Pair<String, Boolean> sanitize(String s) {
    if (s == null) {
      return new Pair<>(null, false);
    }
    StringBuilder sb = new StringBuilder();
    boolean ruleApplied = false;
    for (int i = 0; i < s.length(); i++) {
      char cur = s.charAt(i);
      boolean isLegal = true;
      if (!(44 <= cur && cur <= 57) && !(65 <= cur && cur <= 90) && !(97 <= cur && cur <= 122) &&
          cur != 95) {
        isLegal = false;
        ruleApplied = true;
      }
      sb.append(isLegal ? cur : '-');
    }
    return new Pair<>(sb.toString(), ruleApplied);
  }
}
