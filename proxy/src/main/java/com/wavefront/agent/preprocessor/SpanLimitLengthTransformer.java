package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import wavefront.report.Annotation;
import wavefront.report.Span;

public class SpanLimitLengthTransformer implements Function<Span, Span> {

  private final String scope;
  private final int maxLength;
  private final LengthLimitActionType actionSubtype;
  @Nullable
  private final Pattern compiledMatchPattern;
  private final boolean firstMatchOnly;
  private final PreprocessorRuleMetrics ruleMetrics;

  public SpanLimitLengthTransformer(@Nonnull final String scope,
                                    final int maxLength,
                                    @Nonnull final LengthLimitActionType actionSubtype,
                                    @Nullable final String patternMatch,
                                    final boolean firstMatchOnly,
                                    @Nonnull final PreprocessorRuleMetrics ruleMetrics) {
    this.scope = Preconditions.checkNotNull(scope, "[scope] can't be null");
    Preconditions.checkArgument(!scope.isEmpty(), "[scope] can't be blank");
    if (actionSubtype == LengthLimitActionType.DROP && (scope.equals("spanName") || scope.equals("sourceName"))) {
      throw new IllegalArgumentException("'drop' action type can't be used with spanName and sourceName scope!");
    }
    if (actionSubtype == LengthLimitActionType.TRUNCATE_WITH_ELLIPSIS && maxLength < 3) {
      throw new IllegalArgumentException("'maxLength' must be at least 3 for 'truncateWithEllipsis' action type!");
    }
    Preconditions.checkArgument(maxLength > 0, "[maxLength] needs to be > 0!");
    this.maxLength = maxLength;
    this.actionSubtype = actionSubtype;
    this.compiledMatchPattern = patternMatch != null ? Pattern.compile(patternMatch) : null;
    this.firstMatchOnly = firstMatchOnly;
    this.ruleMetrics = ruleMetrics;
  }

  private String truncate(String input) {
    ruleMetrics.incrementRuleAppliedCounter();
    switch (actionSubtype) {
      case TRUNCATE:
        return input.substring(0, maxLength);
      case TRUNCATE_WITH_ELLIPSIS:
        return input.substring(0, maxLength - 3) + "...";
      default:
        return input;
    }
  }

  public Span apply(@Nonnull Span span) {
    long startNanos = ruleMetrics.ruleStart();
    switch (scope) {
      case "spanName":
        if (compiledMatchPattern == null || compiledMatchPattern.matcher(span.getName()).matches()) {
          span.setName(truncate(span.getName()));
        }
        break;
      case "sourceName":
        if (compiledMatchPattern == null || compiledMatchPattern.matcher(span.getSource()).matches()) {
          span.setSource(truncate(span.getSource()));
        }
        break;
      default:
        List<Annotation> annotations = new ArrayList<>(span.getAnnotations());
        Iterator<Annotation> iterator = annotations.iterator();
        boolean changed = false;
        while (iterator.hasNext()) {
          Annotation entry = iterator.next();
          if (entry.getKey().equals(scope) && entry.getValue().length() > maxLength) {
            if (compiledMatchPattern == null || compiledMatchPattern.matcher(entry.getValue()).matches()) {
              changed = true;
              if (actionSubtype == LengthLimitActionType.DROP) {
                iterator.remove();
                ruleMetrics.incrementRuleAppliedCounter();
              } else {
                entry.setValue(truncate(entry.getValue()));
              }
              if (firstMatchOnly) {
                break;
              }
            }
          }
        }
        if (changed) {
          span.setAnnotations(annotations);
        }
    }
    ruleMetrics.ruleEnd(startNanos);
    return span;
  }
}
