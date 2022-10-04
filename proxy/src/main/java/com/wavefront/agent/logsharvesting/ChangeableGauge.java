package com.wavefront.agent.logsharvesting;

import com.yammer.metrics.core.Gauge;

public class ChangeableGauge<T> extends Gauge<T> {
  private T value;

  public void setValue(T value) {
    this.value = value;
  }

  @Override
  public T value() {
    return this.value;
  }
}
