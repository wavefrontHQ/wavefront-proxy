package com.wavefront.agent.data;

import com.wavefront.api.agent.SpanSamplingPolicy;
import java.util.List;
import javax.annotation.Nullable;

/** Unified interface for non-entity specific dynamic properties, that may change at runtime. */
public interface GlobalProperties {

  /**
   * Get histogram storage accuracy, as specified by the back-end.
   *
   * @return histogram storage accuracy
   */
  short getHistogramStorageAccuracy();

  /**
   * Sets histogram storage accuracy.
   *
   * @param histogramStorageAccuracy storage accuracy
   */
  void setHistogramStorageAccuracy(short histogramStorageAccuracy);

  /**
   * Get the sampling rate for tracing spans.
   *
   * @return sampling rate for tracing spans.
   */
  double getTraceSamplingRate();

  /**
   * Sets the sampling rate for tracing spans.
   *
   * @param traceSamplingRate sampling rate for tracing spans
   */
  void setTraceSamplingRate(@Nullable Double traceSamplingRate);

  /**
   * Get the maximum acceptable duration between now and the end of a span to be accepted for
   * reporting to Wavefront, beyond which they are dropped.
   *
   * @return delay threshold for dropping spans in minutes.
   */
  @Nullable
  Integer getDropSpansDelayedMinutes();

  /**
   * Set the maximum acceptable duration between now and the end of a span to be accepted for
   * reporting to Wavefront, beyond which they are dropped.
   *
   * @param dropSpansDelayedMinutes delay threshold for dropping spans in minutes.
   */
  void setDropSpansDelayedMinutes(@Nullable Integer dropSpansDelayedMinutes);

  /**
   * Get active span sampling policies for policy based sampling.
   *
   * @return list of span sampling policies.
   */
  @Nullable
  List<SpanSamplingPolicy> getActiveSpanSamplingPolicies();

  /**
   * Set active span sampling policies for policy based sampling.
   *
   * @param activeSpanSamplingPolicies list of span sampling policies.
   */
  void setActiveSpanSamplingPolicies(@Nullable List<SpanSamplingPolicy> activeSpanSamplingPolicies);
}
