package com.wavefront.api.agent;

import java.util.UUID;

/**
 * Agent Constants.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public abstract class Constants {

  /**
   * Formatted for graphite head
   */
  public static final String PUSH_FORMAT_GRAPHITE = "graphite";
  /**
   * Formatted for graphite head (without customer id in the metric name).
   */
  public static final String PUSH_FORMAT_GRAPHITE_V2 = "graphite_v2";
  public static final String PUSH_FORMAT_WAVEFRONT = "wavefront"; // alias for graphite_v2

  /**
   * Wavefront histogram format
   */
  public static final String PUSH_FORMAT_HISTOGRAM = "histogram";

  /**
   * Wavefront tracing format
   */
  public static final String PUSH_FORMAT_TRACING = "trace";

  /**
   * Work unit id for blocks of graphite-formatted data.
   */
  public static final UUID GRAPHITE_BLOCK_WORK_UNIT =
      UUID.fromString("12b37289-90b2-4b98-963f-75a27110b8da");
}
