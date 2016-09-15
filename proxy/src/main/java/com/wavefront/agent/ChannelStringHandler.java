package com.wavefront.agent;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import com.wavefront.agent.preprocessor.PointPreprocessor;
import com.wavefront.ingester.Decoder;

import java.net.InetSocketAddress;
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
  private final PointPreprocessor<String> pointLinePreprocessor;
  @Nullable
  private final PointPreprocessor<ReportPoint> reportPointPreprocessor;
  private final PointHandler pointHandler;

  public ChannelStringHandler(Decoder<String> decoder,
                              final int port,
                              final String validationLevel,
                              final int blockedPointsPerBatch,
                              final PostPushDataTimedTask[] postPushDataTimedTasks,
                              @Nullable final PointPreprocessor<String> pointLinePreprocessor,
                              @Nullable final PointPreprocessor<ReportPoint> reportPointPreprocessor) {
    this.decoder = decoder;
    this.pointHandler = new PointHandlerImpl(port, validationLevel, blockedPointsPerBatch, postPushDataTimedTasks);
    this.pointLinePreprocessor = pointLinePreprocessor;
    this.reportPointPreprocessor = reportPointPreprocessor;
  }

  public ChannelStringHandler(Decoder<String> decoder,
                              final PointHandler pointhandler,
                              @Nullable final PointPreprocessor<String> pointLinePreprocessor,
                              @Nullable final PointPreprocessor<ReportPoint> reportPointPreprocessor) {
    this.decoder = decoder;
    this.pointHandler = pointhandler;
    this.pointLinePreprocessor = pointLinePreprocessor;
    this.reportPointPreprocessor = reportPointPreprocessor;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
    processPointLine(msg, decoder, pointHandler, pointLinePreprocessor, reportPointPreprocessor, ctx);
  }

  /**
   * This probably belongs in a base class.  It's only done like this so it can be easily re-used. This should be
   * refactored when it's clear where it belongs.
   */
  public static void processPointLine(final String message,
                                      Decoder<String> decoder,
                                      final PointHandler pointHandler,
                                      @Nullable final PointPreprocessor<String> pointLinePreprocessor,
                                      @Nullable final PointPreprocessor<ReportPoint> reportPointPreprocessor,
                                      @Nullable final ChannelHandlerContext ctx) {
    // ignore empty lines.
    String msg = message;
    if (msg == null || msg.trim().length() == 0) return;

    // transform the line if needed
    if (pointLinePreprocessor != null) {
      msg = pointLinePreprocessor.transform(msg);
    }
    String pointLine = msg.trim();

    // apply white/black lists after formatting
    if (pointLinePreprocessor != null && !pointLinePreprocessor.filter(pointLine)) {
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
      if (ctx != null) {
        InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
        if (remoteAddress != null) {
          errMsg += "; remote: " + remoteAddress.getHostString();
        }
      }
      pointHandler.handleBlockedPoint(errMsg);
    }

    // transform the point after parsing, and apply additional white/black lists if any
    if (reportPointPreprocessor != null) {
      for (ReportPoint point : points) {
        reportPointPreprocessor.transform(point);
        if (!reportPointPreprocessor.filter(point)) {
          pointHandler.handleBlockedPoint(pointLine);
          return;
        }
      }
    }
    pointHandler.reportPoints(points);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (cause.getMessage().equals("Connection reset by peer")) {
      // These errors are caused by the client and are safe to ignore
      return;
    }
    final Throwable rootCause = Throwables.getRootCause(cause);
    String message = "WF-301 Error while receiving data, reason: \""
        + cause.getMessage() + "\"";
    if (rootCause != null && rootCause.getMessage() != null) {
      message += ", root cause: \"" + rootCause.getMessage() + "\"";
    }
    InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
    if (remoteAddress != null) {
      message += "; remote: " + remoteAddress.getHostString();
    }
    pointHandler.handleBlockedPoint(message);
  }
}
