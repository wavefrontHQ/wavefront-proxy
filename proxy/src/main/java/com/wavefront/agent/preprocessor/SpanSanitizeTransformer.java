package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
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
    if (!charactersAreValid(span.getSource())) {
      span.setSource(sanitize(span.getSource()));
      ruleApplied = true;
    }
    if (span.getAnnotations() != null) {
      for (Annotation a : span.getAnnotations()) {
        if (!charactersAreValid(a.getKey())) {
          a.setKey(sanitize(a.getKey()));
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

  private boolean charactersAreValid(String s) {
    if (s == null) {
      return true;
    }
    for (int i = 0; i < s.length(); i++) {
      if (!characterIsValid(s.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  private boolean characterIsValid(char c) {
    // Legal characters are 44-57 (,-./ and numbers), 65-90 (upper), 97-122 (lower), 95 (_)
    return (44 <= c && c <= 57) || (65 <= c && c <= 90) || (97 <= c && c <= 122) || c == 95;
  }

  /**
   * Sanitize a string so that every invalid character is replaced with a dash.
   */
  private String sanitize(String s) {
    if (s == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      sb.append(characterIsValid(c) ? c : '-');
    }
    return sb.toString();
  }
}
