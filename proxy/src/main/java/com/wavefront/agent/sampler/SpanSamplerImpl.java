package com.wavefront.agent.sampler;

import com.wavefront.sdk.entities.tracing.sampling.Sampler;

import java.util.List;
import java.util.UUID;

import wavefront.report.Span;

/**
 * Samples incoming spans and informs whether to report them or not.
 *
 * @author Vikram Raman (vikram@wavefront.com)
 */
public class SpanSamplerImpl implements ReportableEntitySampler<Span> {

  private final List<Sampler> samplers;

  public SpanSamplerImpl(List<Sampler> samplers) {
    this.samplers = samplers;
  }

  @Override
  public boolean sample(Span span) {
    if (samplers == null || samplers.isEmpty()) {
      return true;
    }
    for (Sampler sampler : samplers) {
      if (sampler.sample(span.getName(),
          UUID.fromString(span.getTraceId()).getLeastSignificantBits(), span.getDuration())) {
        return true;
      }
    }
    return false;
  }
}
