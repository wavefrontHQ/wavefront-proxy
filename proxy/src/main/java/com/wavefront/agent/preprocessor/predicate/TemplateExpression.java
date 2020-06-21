package com.wavefront.agent.preprocessor.predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.wavefront.agent.preprocessor.PreprocessorUtil;

import wavefront.report.ReportPoint;
import wavefront.report.Span;

/**
 * A string template rendered. Substitutes {{...}} placeholders with corresponding
 * components; string literals are returned as is.
 *
 * @author vasily@wavefront.com
 */
public class TemplateExpression implements StringExpression {
  private final String template;

  public TemplateExpression(String template) {
    this.template = template;
  }

  @Nonnull
  @Override
  public String getString(@Nullable Object entity) {
    if (entity == null) {
      return template;
    } else if (entity instanceof ReportPoint) {
      return PreprocessorUtil.expandPlaceholders(template, (ReportPoint) entity);
    } else if (entity instanceof Span) {
      return PreprocessorUtil.expandPlaceholders(template, (Span) entity);
    } else {
      throw new IllegalArgumentException("Unknown object type: " +
          entity.getClass().getCanonicalName());
    }
  }
}
