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

  private static ObjectMapper objectMapper = new ObjectMapper();

  public String toString() {
    try {
      return objectMapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      return super.toString();
    }
  }
}
