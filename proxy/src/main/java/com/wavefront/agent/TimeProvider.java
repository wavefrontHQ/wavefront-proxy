package com.wavefront.agent;

/**
 * @author Tim Schmidt (tim@wavefront.com).
 */
public interface TimeProvider {
  long millisSinceEpoch();
}
