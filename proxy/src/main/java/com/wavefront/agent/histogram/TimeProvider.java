package com.wavefront.agent.histogram;

/**
 * @author Tim Schmidt (tim@wavefront.com).
 */
public interface TimeProvider {
  long millisSinceEpoch();
}
