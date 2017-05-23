package com.wavefront.agent.logsharvesting;

import com.yammer.metrics.core.Gauge;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
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
