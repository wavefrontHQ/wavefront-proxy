package com.wavefront.agent.data;

import javax.annotation.Nullable;

import static com.wavefront.agent.data.EntityProperties.DEFAULT_RETRY_BACKOFF_BASE_SECONDS;

/**
 * @author vasily@wavefront.com
 */
public class DefaultGlobalPropertiesForTesting implements GlobalProperties {

  @Override
  public double getRetryBackoffBaseSeconds() {
    return DEFAULT_RETRY_BACKOFF_BASE_SECONDS;
  }

  @Override
  public void setRetryBackoffBaseSeconds(@Nullable Double retryBackoffBaseSeconds) {
  }

  @Override
  public short getHistogramStorageAccuracy() {
    return 32;
  }

  @Override
  public void setHistogramStorageAccuracy(short histogramStorageAccuracy) {
  }

  @Override
  public double getTraceSamplingRate() {
    return 1.0d;
  }

  @Override
  public void setTraceSamplingRate(Double traceSamplingRate) {
  }

  @Override
  public Integer getDropSpansDelayedMinutes() {
    return null;
  }

  @Override
  public void setDropSpansDelayedMinutes(Integer dropSpansDelayedMinutes) {
  }
}
