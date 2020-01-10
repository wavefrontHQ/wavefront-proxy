package com.wavefront.common;

/**
 * @author Tim Schmidt (tim@wavefront.com).
 */
public interface TimeProvider {
  long currentTimeMillis();
}
