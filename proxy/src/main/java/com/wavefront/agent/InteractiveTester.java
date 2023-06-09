package com.wavefront.agent;

import com.wavefront.agent.config.ConfigurationException;

/** Base interface for all interactive testers (logs and preprocessor at the moment). */
public interface InteractiveTester {

  /**
   * Read line from stdin and process it.
   *
   * @return true if there's more input to process
   */
  boolean interactiveTest() throws ConfigurationException;
}
