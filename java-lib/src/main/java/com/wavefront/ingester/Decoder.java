package com.wavefront.ingester;

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import sunnylabs.report.ReportPoint;

/**
 * A decoder of an input line.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public interface Decoder<T> {
  /**
   * Decode graphite points and dump them into an output array. The supplied customer id will be set
   * and no customer id extraction will be attempted.
   *
   * @param msg        Message to parse.
   * @param out        List to output the parsed point.
   * @param customerId The customer id to use as the table for the result ReportPoint.
   */
  void decodeReportPoints(ChannelHandlerContext ctx, T msg, List<ReportPoint> out, String customerId);

  /**
   * Certain decoders support decoding the customer id from the input line itself.
   *
   * @param msg Message to parse.
   * @param out List to output the parsed point.
   */
  void decodeReportPoints(ChannelHandlerContext ctx, T msg, List<ReportPoint> out);
}
