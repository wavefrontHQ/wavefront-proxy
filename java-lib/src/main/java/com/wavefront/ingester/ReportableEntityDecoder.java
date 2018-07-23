package com.wavefront.ingester;

import java.util.List;

import wavefront.report.ReportPoint;

/**
 * A decoder for input data. A more generic version of {@link Decoder},
 * that supports other object types besides {@link ReportPoint}.
 *
 * @author vasily@wavefront.com
 */
public interface ReportableEntityDecoder<T, E> {
  /**
   * Decode entities (points, spans, etc) and dump them into an output array. The supplied customer id will be set
   * and no customer id extraction will be attempted.
   *
   * @param msg        Message to parse.
   * @param out        List to output the parsed point.
   * @param customerId The customer id to use as the table for the resulting entities.
   */
  void decode(T msg, List<E> out, String customerId);

  /**
   * Certain decoders support decoding the customer id from the input line itself.
   *
   * @param msg Message to parse.
   * @param out List to output the parsed point.
   */
  default void decode(T msg, List<E> out) {
    decode(msg, out, "dummy");
  }
}
