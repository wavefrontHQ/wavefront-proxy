package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import wavefront.report.Annotation;
import wavefront.report.Span;

import javax.annotation.Nonnull;

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

  @Override
  public Span apply(@Nonnull Span span) {
    long startNanos = ruleMetrics.ruleStart();
    boolean ruleApplied = false;

    // sanitize name and replace '*' with '-'
    String name = span.getName();
    if (name != null) {
      span.setName(sanitizeValue(name).replace('*', '-'));
      if (span.getName().equals(name)) {
        ruleApplied = true;
      }
    }

    // sanitize source
    String source = span.getSource();
    if (source != null) {
      span.setSource(sanitize(source));
      if (!ruleApplied && !span.getSource().equals(source)) {
        ruleApplied = true;
      }
    }

    if (span.getAnnotations() != null) {
      for (Annotation a : span.getAnnotations()) {
        // sanitize tag key
        String key = a.getKey();
        if (key != null) {
          a.setKey(sanitize(key));
          if (!ruleApplied && !a.getKey().equals(key)) {
            ruleApplied = true;
          }
        }

        // sanitize tag value
        String value = a.getValue();
        if (value != null) {
          a.setValue(sanitizeValue(value));
          if (!ruleApplied && !a.getValue().equals(value)) {
            ruleApplied = true;
          }
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
   */
  private String sanitize(String s) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      // Legal characters are 44-57 (,-./ and numbers), 65-90 (upper), 97-122 (lower), 95 (_)
      if ((44 <= c && c <= 57) || (65 <= c && c <= 90) || (97 <= c && c <= 122) || c == 95) {
        sb.append(c);
      } else {
        sb.append('-');
      }
    }
    return sb.toString();
  }

  /**
   * Remove leading/trailing whitespace and escape newlines.
   */
  private String sanitizeValue(String s) {
    return s.trim().replaceAll("\\n", "\\\\n");
  }
}
