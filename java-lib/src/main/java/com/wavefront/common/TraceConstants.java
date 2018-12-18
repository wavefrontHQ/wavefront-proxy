package com.wavefront.common;

/**
 * Trace constants.
 *
 * @author Anil Kodali (akodali@vmware.com)
 */
public abstract class TraceConstants {
  // Span References
  // See https://opentracing.io/specification/ for more information about span references.
  public static final String FOLLOWS_FROM_KEY = "followsFrom";
  public static final String PARENT_KEY = "parent";
}
