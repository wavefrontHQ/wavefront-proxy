package com.wavefront.agent.handlers;

import org.apache.commons.lang.StringUtils;

import java.util.Collection;

/**
 * A collection of helper methods around plaintext newline-delimited payloads.
 *
 * @author vasily@wavefront.com
 */
public class LineDelimitedUtils {
  public static final String PUSH_DATA_DELIMETER = "\n";

  private LineDelimitedUtils() {
  }

  /**
   * Split a newline-delimited payload into a string array.
   *
   * @param pushData payload to split.
   * @return string array
   */
  public static String[] splitPushData(String pushData) {
    return StringUtils.split(pushData, PUSH_DATA_DELIMETER);
  }

  /**
   * Join a batch of strings into a payload string.
   *
   * @param pushData collection of strings.
   * @return payload
   */
  public static String joinPushData(Collection<String> pushData) {
    return StringUtils.join(pushData, PUSH_DATA_DELIMETER);
  }

  /**
   * Calculates the number of points in the pushData payload.
   *
   * @param pushData a newline-delimited payload string.
   * @return number of points
   */
  public static int pushDataSize(String pushData) {
    int length = StringUtils.countMatches(pushData, PUSH_DATA_DELIMETER);
    return length > 0
        ? length + 1
        : (pushData.length() > 0 ? 1 : 0);
  }
}
