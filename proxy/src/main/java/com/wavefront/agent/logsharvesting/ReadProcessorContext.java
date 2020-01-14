package com.wavefront.agent.logsharvesting;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class ReadProcessorContext {
  private final Double value;

  public ReadProcessorContext(Double value) {
    this.value = value;
  }

  public Double getValue() {
    return value;
  }
}
