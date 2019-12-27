package com.wavefront.agent;

/**
 * Background process that can be started and stopped.
 *
 * @author vasily@wavefront.com
 */
public interface Managed {
  void start();

  void stop();
}
