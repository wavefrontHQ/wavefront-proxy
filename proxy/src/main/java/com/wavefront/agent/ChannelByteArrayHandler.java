package com.wavefront.agent;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import com.wavefront.common.MetricWhiteBlackList;
import com.wavefront.ingester.Decoder;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import sunnylabs.report.ReportPoint;


/**
 * Channel handler for byte array data.
 * @author Mike McLaughlin (mike@wavefront.com)
 */
@ChannelHandler.Sharable
class ChannelByteArrayHandler extends SimpleChannelInboundHandler<byte[]> {
  private static final Logger logger = Logger.getLogger(
      ChannelByteArrayHandler.class.getCanonicalName());

  private final Decoder<byte[]> decoder;
  private final PointHandler pointHandler;

  private final Predicate<String> metricPredicate;

  /**
   * Constructor.
   */
  ChannelByteArrayHandler(Decoder<byte[]> decoder,
                                 final int port,
                                 final String prefix,
                                 final String validationLevel,
                                 final int blockedPointsPerBatch,
                                 final PostPushDataTimedTask[] postPushDataTimedTasks,
                                 @Nullable final String pointLineWhiteListRegex,
                                 @Nullable final String pointLineBlackListRegex) {
    this.decoder = decoder;
    this.pointHandler = new PointHandlerImpl(port, validationLevel, blockedPointsPerBatch, prefix,
        postPushDataTimedTasks);
    this.metricPredicate = new MetricWhiteBlackList(
      pointLineWhiteListRegex, pointLineBlackListRegex, String.valueOf(port));
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, byte[] msg) throws Exception {
    // ignore empty lines.
    if (msg == null || msg.length == 0) {
      return;
    }

    List<ReportPoint> points = Lists.newArrayListWithExpectedSize(1);
    try {
      decoder.decodeReportPoints(msg, points, "dummy");
      for (final ReportPoint point: points) {
        if (!this.metricPredicate.apply(point.getMetric())) {
          pointHandler.handleBlockedPoint(point.getMetric());
          continue;
        }
        pointHandler.reportPoint(point, point.getMetric());
      }
    } catch (final Exception e) {
      final Throwable rootCause = Throwables.getRootCause(e);
      String errMsg = "WF-300 Cannot parse: \"" +
          "\", reason: \"" + e.getMessage() + "\"";
      if (rootCause != null && rootCause.getMessage() != null) {
        errMsg = errMsg + ", root cause: \"" + rootCause.getMessage() + "\"";
      }
      logger.log(Level.WARNING, errMsg, e);
      pointHandler.handleBlockedPoint(errMsg);
    }
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
    pointHandler.handleBlockedPoint(message);
  }
}
