package com.wavefront.agent.sampler;

import com.wavefront.sdk.entities.tracing.sampling.DurationSampler;
import com.wavefront.sdk.entities.tracing.sampling.RateSampler;
import com.wavefront.sdk.entities.tracing.sampling.Sampler;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * Helper class for creating span samplers.
 *
 * @author Vikram Raman (vikram@wavefront.com)
 */
public class SpanSamplerUtils {

  @Nullable
  public static Sampler getRateSampler(double rate) {
    if (rate < 0.0 || rate >= 1.0) {
      return null;
    }
    return new RateSampler(rate);
  }

  @Nullable
  public static Sampler getDurationSampler(int duration) {
    if (duration == 0) {
      return null;
    }
    return new DurationSampler(duration);
  }

  @Nullable
  public static List<Sampler> fromSamplers(Sampler... samplers) {
    if (samplers == null || samplers.length == 0) {
      return null;
    }
    return Arrays.stream(samplers).filter(Objects::nonNull).collect(Collectors.toList());
  }
}
