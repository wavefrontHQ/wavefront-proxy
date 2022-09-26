package com.wavefront.agent.data;

import com.wavefront.api.agent.SpanSamplingPolicy;
import java.util.List;
import javax.annotation.Nullable;

/** @author vasily@wavefront.com */
public class DefaultGlobalPropertiesForTesting implements GlobalProperties {

  @Override
  public short getHistogramStorageAccuracy() {
    return 32;
  }

  @Override
  public void setHistogramStorageAccuracy(short histogramStorageAccuracy) {}

  @Override
  public double getTraceSamplingRate() {
    return 1.0d;
  }

  @Override
  public void setTraceSamplingRate(Double traceSamplingRate) {}

  @Override
  public Integer getDropSpansDelayedMinutes() {
    return null;
  }

  @Override
  public void setDropSpansDelayedMinutes(Integer dropSpansDelayedMinutes) {}

  @Override
  public List<SpanSamplingPolicy> getActiveSpanSamplingPolicies() {
    return null;
  }

  @Override
  public void setActiveSpanSamplingPolicies(
      @Nullable List<SpanSamplingPolicy> activeSpanSamplingPolicies) {}
}
