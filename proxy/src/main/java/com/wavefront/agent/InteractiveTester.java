package com.wavefront.agent;

import com.wavefront.agent.config.ConfigurationException;

/**
 * Base interface for all interactive testers (logs and preprocessor at the moment).
 *
 * @author vasily@wavefront.com
 */
public interface InteractiveTester {

  /**
   * Read line from stdin and process it.
   *
   * @return true if there's more input to process
   * @throws ConfigurationException
   */
  boolean interactiveTest() throws ConfigurationException;
}
