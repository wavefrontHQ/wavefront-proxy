package com.wavefront.agent.handlers;

import org.apache.commons.lang.StringUtils;

import java.util.Collection;

/**
 * A collection of helper methods around plaintext newline-delimited payloads.
 *
 * @author vasily@wavefront.com
 */
public abstract class LineDelimitedUtils {
  static final String PUSH_DATA_DELIMITER = "\n";

  private LineDelimitedUtils() {
  }

  /**
   * Split a newline-delimited payload into a string array.
   *
   * @param pushData payload to split.
   * @return string array
   */
  @Deprecated
  public static String[] splitPushData(String pushData) {
    return StringUtils.split(pushData, PUSH_DATA_DELIMITER);
  }

  /**
   * Join a batch of strings into a payload string.
   *
   * @param pushData collection of strings.
   * @return payload
   */
  public static String joinPushData(Collection<String> pushData) {
    return StringUtils.join(pushData, PUSH_DATA_DELIMITER);
  }
}
