package com.wavefront.common;

/**
 * Background process that can be started and stopped.
 *
 * @author vasily@wavefront.com
 */
public interface Managed {
  /**
   * Starts the process.
   */
  void start();

  /**
   * Stops the process.
   */
  void stop();
}
