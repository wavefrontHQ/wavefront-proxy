package com.wavefront.agent.formatter;

import com.wavefront.ingester.AbstractIngesterFormatter;
import com.wavefront.ingester.EventDecoder;
import com.wavefront.ingester.ReportSourceTagDecoder;

/**
 * Best-effort data format auto-detection.
 *
 * @author vasily@wavefront.com
 */
public enum DataFormat {
  GENERIC, HISTOGRAM, SOURCE_TAG, EVENT, JSON_STRING;

  public static DataFormat autodetect(final String input) {
    if (input.length() < 2) return GENERIC;
    char firstChar = input.charAt(0);
    switch (firstChar) {
      case '@':
        if (input.startsWith(AbstractIngesterFormatter.SOURCE_TAG_LITERAL) ||
            input.startsWith(AbstractIngesterFormatter.SOURCE_DESCRIPTION_LITERAL)) {
          return SOURCE_TAG;
        }
        if (input.startsWith(EventDecoder.EVENT)) return EVENT;
        break;
      case '{':
        if (input.charAt(input.length() - 1) == '}') return JSON_STRING;
        break;
      case '!':
        if (input.startsWith("!M ") || input.startsWith("!H ") || input.startsWith("!D ")) {
          return HISTOGRAM;
        }
        break;
    }
    return GENERIC;
  }
}
