package com.wavefront.agent.sampler;

import static com.wavefront.internal.SpanDerivedMetricsUtils.ERROR_SPAN_TAG_KEY;
import static com.wavefront.internal.SpanDerivedMetricsUtils.ERROR_SPAN_TAG_VAL;

import com.wavefront.sdk.entities.tracing.sampling.Sampler;
import com.yammer.metrics.core.Counter;
import wavefront.report.Span;

import javax.annotation.Nullable;
import java.util.UUID;

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
    if (alwaysSampleErrors && span.getAnnotations().stream().anyMatch(t ->
        t.getKey().equals(ERROR_SPAN_TAG_KEY) && t.getValue().equals(ERROR_SPAN_TAG_VAL))) {
      return true;
    } else if (delegate.sample(span.getName(),
        UUID.fromString(span.getTraceId()).getLeastSignificantBits(), span.getDuration())) {
      return true;
    }
    if (discarded != null) {
      discarded.inc();
    }
    return false;
  }
}
