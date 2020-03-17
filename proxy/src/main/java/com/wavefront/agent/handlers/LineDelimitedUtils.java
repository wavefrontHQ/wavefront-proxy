package com.wavefront.agent.handlers;

import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

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
  @Deprecated
  public static String[] splitPushData(String pushData) {
    return StringUtils.split(pushData, PUSH_DATA_DELIMETER);
  }

  /**
   * Iterate over individual strings in a newline-delimited string. Skips empty strings.
   *
   * @param input payload to split.
   * @return string iterator
   */
  public static Iterator<String> splitStringIterator(String input, char delimiter) {
    return new Iterator<String>() {
      int currentPos = 0;
      int indexOfDelimiter = input.indexOf(delimiter);
      String peek = null;

      @Override
      public boolean hasNext() {
        if (peek == null) peek = advance();
        return peek != null;
      }

      @Override
      public String next() {
        try {
          if (peek == null) peek = advance();
          if (peek == null) throw new NoSuchElementException();
          return peek;
        } finally {
          peek = null;
        }
      }

      private String advance() {
        String result = "";
        while ("".equals(result)) {
          result = internalNext();
        }
        return result;
      }

      private String internalNext() {
        if (indexOfDelimiter >= 0) {
          try {
            return input.substring(currentPos, indexOfDelimiter);
          } finally {
            currentPos = indexOfDelimiter + 1;
            indexOfDelimiter = input.indexOf(delimiter, currentPos);
          }
        } else if (indexOfDelimiter == -1) {
          indexOfDelimiter = Integer.MIN_VALUE;
          return input.substring(currentPos);
        } else {
          return null;
        }
      }
    };
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
}
