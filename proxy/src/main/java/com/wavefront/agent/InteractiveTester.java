package com.wavefront.agent;

import com.wavefront.agent.config.ConfigurationException;

/**
 *
 * @author vasily@wavefront.com
 */
public interface InteractiveTester {

  /**
   *
   *
   * @return
   * @throws ConfigurationException
   */
  boolean interactiveTest() throws ConfigurationException;
}
