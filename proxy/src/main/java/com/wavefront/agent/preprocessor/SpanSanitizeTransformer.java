package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import wavefront.report.Annotation;
import wavefront.report.Span;

import javax.annotation.Nullable;

import static com.wavefront.sdk.common.Utils.sanitizeWithoutQuotes;

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
  public Span apply(@Nullable Span span) {
    if (span == null) return null;
    long startNanos = ruleMetrics.ruleStart();
    boolean ruleApplied = false;

    // sanitize name and replace '*' with '-'
    String name = span.getName();
    if (name != null) {
      span.setName(sanitizeValue(name).replace('*', '-'));
      if (!span.getName().equals(name)) {
        ruleApplied = true;
      }
    }

    // sanitize source
    String source = span.getSource();
    if (source != null) {
      span.setSource(sanitizeWithoutQuotes(source));
      if (!span.getSource().equals(source)) {
        ruleApplied = true;
      }
    }

    if (span.getAnnotations() != null) {
      for (Annotation a : span.getAnnotations()) {
        // sanitize tag key
        String key = a.getKey();
        if (key != null) {
          a.setKey(sanitizeWithoutQuotes(key));
          if (!a.getKey().equals(key)) {
            ruleApplied = true;
          }
        }

        // sanitize tag value
        String value = a.getValue();
        if (value != null) {
          a.setValue(sanitizeValue(value));
          if (!a.getValue().equals(value)) {
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
   * Remove leading/trailing whitespace and escape newlines.
   */
  private String sanitizeValue(String s) {
    // TODO: sanitize using SDK instead
    return s.trim().replaceAll("\\n", "\\\\n");
  }
}
