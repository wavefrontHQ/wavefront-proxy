package com.wavefront.agent.config;

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
}
