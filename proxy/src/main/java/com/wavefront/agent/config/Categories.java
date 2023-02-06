package com.wavefront.agent.config;

import com.fasterxml.jackson.annotation.JsonValue;

public enum Categories {
  GENERAL("General", 1),
  INPUT("Input", 2),
  BUFFER("Buffering", 3),
  OUTPUT("Output", 4),
  TRACE("Trace", 5),
  NA("Others", 9999); // for hided options

  private final String value;
  private int order;

  Categories(String value, int order) {
    this.value = value;
    this.order = order;
  }

  @JsonValue
  public String getValue() {
    return value;
  }

  public int getOrder() {
    return order;
  }
}
