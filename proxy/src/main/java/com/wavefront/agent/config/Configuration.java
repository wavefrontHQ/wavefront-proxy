package com.wavefront.agent.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public abstract class Configuration {
  protected void ensure(boolean b, String message) throws ConfigurationException {
    if (!b) {
      throw new ConfigurationException(message);
    }
  }

  public abstract void verifyAndInit() throws ConfigurationException;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public String toString() {
    try {
      return OBJECT_MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    return getClass().equals(other.getClass()) && toString().equals(other.toString());
  }
}
