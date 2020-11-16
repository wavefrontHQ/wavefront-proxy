package com.wavefront.agent.data;

import javax.annotation.Nullable;

import com.wavefront.agent.ProxyConfig;

import static com.wavefront.agent.config.ReportableConfig.reportSettingAsGauge;
import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

/**
 * Dynamic non-entity specific properties, that may change at runtime.
 *
 * @author vasily@wavefront.com
 */
public final class GlobalPropertiesImpl implements GlobalProperties {
  private final ProxyConfig wrapped;
  private Double retryBackoffBaseSeconds = null;
  private short histogramStorageAccuracy = 32;
  private Double traceSamplingRate = null;
  private Integer dropSpansDelayedMinutes = null;

  public GlobalPropertiesImpl(ProxyConfig wrapped) {
    this.wrapped = wrapped;
    reportSettingAsGauge(this::getRetryBackoffBaseSeconds, "dynamic.retryBackoffBaseSeconds");
  }

  @Override
  public double getRetryBackoffBaseSeconds() {
    return firstNonNull(retryBackoffBaseSeconds, wrapped.getRetryBackoffBaseSeconds());
  }

  @Override
  public void setRetryBackoffBaseSeconds(@Nullable Double retryBackoffBaseSeconds) {
    this.retryBackoffBaseSeconds = retryBackoffBaseSeconds;
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
}

