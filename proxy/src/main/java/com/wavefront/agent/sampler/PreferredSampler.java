package com.wavefront.agent.sampler;

import javax.annotation.Nonnull;
import wavefront.report.Span;

/** This interface is for preferred sampler which if not null will override other samplers */
public interface PreferredSampler {
  boolean sample(@Nonnull Span span);

  boolean isApplicable(@Nonnull Span span);
}
