package com.wavefront.agent.data;

import javax.annotation.Nullable;

/**
 * Unified interface for non-entity specific dynamic properties, that may change at runtime.
 *
 * @author vasily@wavefront.com
 */
public interface GlobalProperties {
  /**
   * Get base in seconds for retry thread exponential backoff.
   *
   * @return exponential backoff base value
   */
  double getRetryBackoffBaseSeconds();

  /**
   * Sets base in seconds for retry thread exponential backoff.
   *
   * @param retryBackoffBaseSeconds new value for exponential backoff base value.
   *                                if null is provided, reverts to originally configured value.
   */
  void setRetryBackoffBaseSeconds(@Nullable Double retryBackoffBaseSeconds);

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
}
