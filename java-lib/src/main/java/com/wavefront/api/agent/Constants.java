package com.wavefront.api.agent;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.UUID;

import javax.validation.constraints.NotNull;

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
   * Work unit id for blocks of graphite-formatted data.
   */
  public static final UUID GRAPHITE_BLOCK_WORK_UNIT =
      UUID.fromString("12b37289-90b2-4b98-963f-75a27110b8da");

  /**
   * Map of constant aliases, i.e. "wavefront" is an alias of "graphite_v2"
   */
  private static final Map<String, String> aliases = ImmutableMap.of(PUSH_FORMAT_WAVEFRONT, PUSH_FORMAT_GRAPHITE_V2);

  /**
   * Tests if value equals to or is a valid alias of the constant
   *
   * @param value    value to test
   * @param constant constant to test the value against
   * @return true if equals to or is a valid alias of
   */
  public static boolean is(String value, @NotNull String constant) {
    return constant != null && (constant.equals(value) || constant.equals(aliases.get(value)));
  }
}
