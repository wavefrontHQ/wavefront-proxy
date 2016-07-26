package com.wavefront.agent;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import com.wavefront.common.MetricWhiteBlackList;
import com.wavefront.ingester.Decoder;

import java.util.List;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import sunnylabs.report.ReportPoint;

/**
 * Parses points from a channel using the given decoder and send it off to the AgentAPI interface.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
@ChannelHandler.Sharable
public class ChannelStringHandler extends SimpleChannelInboundHandler<String> {

  private static final Logger logger = Logger.getLogger(ChannelStringHandler.class.getCanonicalName());

  private final Decoder<String> decoder;

  /**
   * Transformer to transform each line.
   */
  @Nullable
  private final Function<String, String> transformer;
  private final PointHandler pointHandler;

  private final Predicate<String> linePredicate;

  public ChannelStringHandler(Decoder<String> decoder,
                              final int port,
                              final String prefix,
                              final String validationLevel,
                              final int blockedPointsPerBatch,
                              final PostPushDataTimedTask[] postPushDataTimedTasks,
                              @Nullable final Function<String, String> transformer,
                              @Nullable final String pointLineWhiteListRegex,
                              @Nullable final String pointLineBlackListRegex) {
    this.decoder = decoder;
    this.pointHandler = new PointHandlerImpl(port, validationLevel, blockedPointsPerBatch, prefix,
        postPushDataTimedTasks);
    this.transformer = transformer;
    this.linePredicate = new MetricWhiteBlackList(pointLineWhiteListRegex,
        pointLineBlackListRegex,
        String.valueOf(port));
  }

  public ChannelStringHandler(Decoder<String> decoder,
                              final PointHandler pointhandler,
                              final Predicate<String> linePredicate,
                              @Nullable final Function<String, String> transformer) {
    this.decoder = decoder;
    this.pointHandler = pointhandler;
    this.transformer = transformer;
    this.linePredicate = linePredicate;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
    processPointLine(msg, decoder, pointHandler, linePredicate, transformer);
  }

  /**
   * This probably belongs in a base class.  It's only done like this so it can be easily re-used. This should be
   * refactored when it's clear where it belongs.
   */
  public static void processPointLine(final String message,
                                      Decoder<String> decoder,
                                      final PointHandler pointHandler,
                                      final Predicate<String> linePredicate,
                                      @Nullable final Function<String, String> transformer) {
    // ignore empty lines.
    String msg = message;
    if (msg == null || msg.trim().length() == 0) return;

    // transform the line if needed
    if (transformer != null) {
      msg = transformer.apply(msg);
    }
    String pointLine = msg.trim();

    // apply white/black lists after formatting
    if (!linePredicate.apply(pointLine)) {
      pointHandler.handleBlockedPoint(pointLine);
      return;
    }

    // decode the line into report points
    List<ReportPoint> points = Lists.newArrayListWithExpectedSize(1);
    try {
      decoder.decodeReportPoints(pointLine, points, "dummy");
    } catch (Exception e) {
      final Throwable rootCause = Throwables.getRootCause(e);
      String errMsg = "WF-300 Cannot parse: \"" + pointLine +
          "\", reason: \"" + e.getMessage() + "\"";
      if (rootCause != null && rootCause.getMessage() != null) {
        errMsg = errMsg + ", root cause: \"" + rootCause.getMessage() + "\"";
      }
      pointHandler.handleBlockedPoint(errMsg);
    }
    pointHandler.reportPoints(points);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    final Throwable rootCause = Throwables.getRootCause(cause);
    String message = "WF-301 Channel Handler Failed, reason: \""
        + cause.getMessage() + "\"";
    if (rootCause != null && rootCause.getMessage() != null) {
      message += ", root cause: \"" + rootCause.getMessage() + "\"";
    }
    pointHandler.handleBlockedPoint(message);
  }
}
