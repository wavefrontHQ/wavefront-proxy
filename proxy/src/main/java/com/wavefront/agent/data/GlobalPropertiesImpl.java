package com.wavefront.agent.data;

import com.wavefront.agent.ProxyConfig;
import com.wavefront.api.agent.SpanSamplingPolicy;
import java.util.List;
import javax.annotation.Nullable;

/** Dynamic non-entity specific properties, that may change at runtime. */
public final class GlobalPropertiesImpl implements GlobalProperties {
  private final ProxyConfig wrapped;
  private Double retryBackoffBaseSeconds = null;
  private short histogramStorageAccuracy = 32;
  private Double traceSamplingRate = null;
  private Integer dropSpansDelayedMinutes = null;
  private List<SpanSamplingPolicy> activeSpanSamplingPolicies;

  public GlobalPropertiesImpl(ProxyConfig wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public short getHistogramStorageAccuracy() {
    return histogramStorageAccuracy;
  }

  @Override
  public void setHistogramStorageAccuracy(short histogramStorageAccuracy) {
    this.histogramStorageAccuracy = histogramStorageAccuracy;
  }

  @Override
  public double getTraceSamplingRate() {
    if (traceSamplingRate != null) {
      // use the minimum of backend provided and local proxy configured sampling rates.
      return Math.min(traceSamplingRate, wrapped.getTraceSamplingRate());
    } else {
      return wrapped.getTraceSamplingRate();
    }
  }

  public void setTraceSamplingRate(@Nullable Double traceSamplingRate) {
    this.traceSamplingRate = traceSamplingRate;
  }

  @Override
  public Integer getDropSpansDelayedMinutes() {
    return dropSpansDelayedMinutes;
  }

  @Override
  public void setDropSpansDelayedMinutes(@Nullable Integer dropSpansDelayedMinutes) {
    this.dropSpansDelayedMinutes = dropSpansDelayedMinutes;
  }

  @Override
  public List<SpanSamplingPolicy> getActiveSpanSamplingPolicies() {
    return activeSpanSamplingPolicies;
  }

  @Override
  public void setActiveSpanSamplingPolicies(
      @Nullable List<SpanSamplingPolicy> activeSpanSamplingPolicies) {
    this.activeSpanSamplingPolicies = activeSpanSamplingPolicies;
  }
}
