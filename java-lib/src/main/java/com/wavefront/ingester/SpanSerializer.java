package com.wavefront.ingester;

import org.apache.commons.lang.StringUtils;

import java.util.function.Function;

import wavefront.report.Annotation;
import wavefront.report.Span;

/**
 * Convert a {@link Span} to its string representation in a canonical format (quoted name and annotations).
 *
 * @author vasily@wavefront.com
 */
public class SpanSerializer implements Function<Span, String> {

  @Override
  public String apply(Span span) {
    return spanToString(span);
  }

  private static String quote = "\"";
  private static String escapedQuote = "\\\"";

  private static String escapeQuotes(String raw) {
    return StringUtils.replace(raw, quote, escapedQuote);
  }

  static String spanToString(Span span) {
    StringBuilder sb = new StringBuilder(quote)
        .append(escapeQuotes(span.getName())).append(quote).append(' ');
    if (span.getSource() != null) {
      sb.append("source=").append(quote).append(escapeQuotes(span.getSource())).append(quote).append(' ');
    }
    if (span.getSpanId() != null) {
      sb.append("spanId=").append(quote).append(escapeQuotes(span.getSpanId())).append(quote).append(' ');
    }
    if (span.getTraceId() != null) {
      sb.append("traceId=").append(quote).append(escapeQuotes(span.getTraceId())).append(quote);
    }
    if (span.getAnnotations() != null) {
      for (Annotation entry : span.getAnnotations()) {
        sb.append(' ').append(quote).append(escapeQuotes(entry.getKey())).append(quote)
            .append("=")
            .append(quote).append(escapeQuotes(entry.getValue())).append(quote);
      }
    }
    sb.append(' ')
        .append(span.getStartMillis())
        .append(' ')
        .append(span.getDuration());
    return sb.toString();
  }
}

