package com.wavefront.agent.formatter;

import com.wavefront.api.agent.Constants;
import com.wavefront.ingester.AbstractIngesterFormatter;
import javax.annotation.Nullable;

/** Best-effort data format auto-detection. */
public enum DataFormat {
  DEFAULT,
  WAVEFRONT,
  HISTOGRAM,
  SOURCE_TAG,
  EVENT,
  SPAN,
  SPAN_LOG,
  LOGS_JSON_ARR,
  LOGS_JSON_LINES,
  LOGS_JSON_CLOUDWATCH;

  public static DataFormat autodetect(final String input) {
    if (input.length() < 2) return DEFAULT;
    char firstChar = input.charAt(0);
    switch (firstChar) {
      case '@':
        if (input.startsWith(AbstractIngesterFormatter.SOURCE_TAG_LITERAL)
            || input.startsWith(AbstractIngesterFormatter.SOURCE_DESCRIPTION_LITERAL)) {
          return SOURCE_TAG;
        }
        if (input.startsWith(AbstractIngesterFormatter.EVENT_LITERAL)) return EVENT;
        break;
      case '{':
        if (input.charAt(input.length() - 1) == '}') return SPAN_LOG;
        break;
      case '!':
        if (input.startsWith("!M ") || input.startsWith("!H ") || input.startsWith("!D ")) {
          return HISTOGRAM;
        }
        break;
      case '[':
        if (input.charAt(input.length() - 1) == ']') return LOGS_JSON_ARR;
        break;
    }
    return DEFAULT;
  }

  @Nullable
  public static DataFormat parse(String format) {
    if (format == null) return null;
    switch (format) {
      case Constants.PUSH_FORMAT_WAVEFRONT:
      case Constants.PUSH_FORMAT_GRAPHITE_V2:
        return DataFormat.WAVEFRONT;
      case Constants.PUSH_FORMAT_HISTOGRAM:
        return DataFormat.HISTOGRAM;
      case Constants.PUSH_FORMAT_TRACING:
        return DataFormat.SPAN;
      case Constants.PUSH_FORMAT_TRACING_SPAN_LOGS:
        return DataFormat.SPAN_LOG;
      case Constants.PUSH_FORMAT_LOGS_JSON_ARR:
        return DataFormat.LOGS_JSON_ARR;
        // TODO: review
        //      case Constants.PUSH_FORMAT_LOGS_JSON_LINES:
        //        return DataFormat.LOGS_JSON_LINES;
        //      case Constants.PUSH_FORMAT_LOGS_JSON_CLOUDWATCH:
        //        return DataFormat.LOGS_JSON_CLOUDWATCH;
      default:
        return null;
    }
  }
}
