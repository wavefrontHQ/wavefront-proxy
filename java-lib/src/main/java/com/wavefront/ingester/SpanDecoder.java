package com.wavefront.ingester;

import com.google.common.base.Preconditions;

import java.util.List;

import wavefront.report.Span;

/**
 * Span decoder that takes in data in the following format:
 *
 * [span name] [annotations] [timestamp] [duration|timestamp]
 *
 * @author vasily@wavefront.com
 */
public class SpanDecoder implements ReportableEntityDecoder<String, Span> {

  private final String hostName;
  private static final AbstractIngesterFormatter<Span> FORMAT =
      SpanIngesterFormatter.newBuilder()
          .whiteSpace()
          .appendName().whiteSpace()
          .appendBoundedAnnotationsConsumer().whiteSpace()
          .appendRawTimestamp().whiteSpace()
          .appendDuration().whiteSpace()
          .build();

  public SpanDecoder(String hostName) {
    Preconditions.checkNotNull(hostName);
    this.hostName = hostName;
  }

  @Override
  public void decode(String msg, List<Span> out, String customerId) {
    Span span = FORMAT.drive(msg, hostName, customerId);
    if (out != null) {
      out.add(span);
    }
  }
}
