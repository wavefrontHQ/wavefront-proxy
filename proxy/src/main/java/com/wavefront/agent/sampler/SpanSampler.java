package com.wavefront.agent.sampler;

import static com.wavefront.internal.SpanDerivedMetricsUtils.DEBUG_SPAN_TAG_VAL;
import static com.wavefront.internal.SpanDerivedMetricsUtils.ERROR_SPAN_TAG_KEY;
import static com.wavefront.internal.SpanDerivedMetricsUtils.ERROR_SPAN_TAG_VAL;
import static com.wavefront.sdk.common.Constants.DEBUG_TAG_KEY;
import static com.wavefront.sdk.common.Constants.ERROR_TAG_KEY;

import com.wavefront.sdk.entities.tracing.sampling.Sampler;
import com.yammer.metrics.core.Counter;

import wavefront.report.Annotation;
import wavefront.report.Span;

import javax.annotation.Nullable;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;

/**
 * Sampler that takes a {@link Span} as input and delegates to a {@link Sampler} when evaluating the
 * sampling decision.
 *
 * @author Han Zhang (zhanghan@vmware.com)
 */
public class SpanSampler {
  private final Sampler delegate;
  private final boolean alwaysSampleErrors;

  /**
   * Creates a new instance from a {@Sampler} delegate.
   *
   * @param delegate            The delegate {@Sampler}.
   * @param alwaysSampleErrors  Whether to always sample spans that have error tag set to true.
   */
  public SpanSampler(Sampler delegate, boolean alwaysSampleErrors) {
    this.delegate = delegate;
    this.alwaysSampleErrors = alwaysSampleErrors;
  }

  /**
   * Evaluates whether a span should be allowed or discarded.
   *
   * @param span  The span to sample.
   * @return true if the span should be allowed, false otherwise.
   */
  public boolean sample(Span span) {
    return sample(span, null);
  }

  /**
   * Evaluates whether a span should be allowed or discarded, and increment a counter if it should
   * be discarded.
   *
   * @param span      The span to sample.
   * @param discarded The counter to increment if the decision is to discard the span.
   * @return true if the span should be allowed, false otherwise.
   */
  public boolean sample(Span span, @Nullable Counter discarded) {
    if (isForceSampled(span)) {
      return true;
    }
    if (delegate.sample(span.getName(), UUID.fromString(span.getTraceId()).getLeastSignificantBits(),
        span.getDuration())) {
      return true;
    }
    if (discarded != null) {
      discarded.inc();
    }
    return false;
  }

  /**
   * Util method to determine if a span is force sampled.
   * Currently force samples if any of the below conditions are met.
   * 1. The span annotation debug=true is present
   * 2. alwaysSampleErrors=true and the span annotation error=true is present.
   *
   * @param span The span to sample
   * @return true if the span should be force sampled.
   */
  private boolean isForceSampled(Span span) {
    List<Annotation> annotations = span.getAnnotations();
    for (Annotation annotation : annotations) {
      switch (annotation.getKey()) {
        case DEBUG_TAG_KEY:
          if(annotation.getValue().equals(DEBUG_SPAN_TAG_VAL)) {
            return true;
          }
          break;
        case ERROR_TAG_KEY:
          if(alwaysSampleErrors && annotation.getValue().equals(ERROR_SPAN_TAG_VAL)) {
            return true;
          }
          break;
      }
    }
    return false;
  }
}
